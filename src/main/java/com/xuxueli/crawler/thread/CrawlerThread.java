package com.xuxueli.crawler.thread;

import com.xuxueli.crawler.XxlCrawler;
import com.xuxueli.crawler.annotation.PageFieldSelect;
import com.xuxueli.crawler.annotation.PageSelect;
import com.xuxueli.crawler.conf.XxlCrawlerConf;
import com.xuxueli.crawler.exception.XxlCrawlerException;
import com.xuxueli.crawler.model.PageRequest;
import com.xuxueli.crawler.parser.strategy.NonPageParser;
import com.xuxueli.crawler.util.FieldReflectionUtil;
import com.xuxueli.crawler.util.JsoupUtil;
import com.xuxueli.crawler.util.UrlUtil;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * crawler thread
 *
 * @author xuxueli 2017-10-10 10:58:19
 */
public class CrawlerThread implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(CrawlerThread.class);

    private XxlCrawler crawler;
    private boolean running;
    private boolean toStop;

    public CrawlerThread(XxlCrawler crawler) {
        this.crawler = crawler;
        this.running = true;
        this.toStop = false;
    }

    public void toStop() {
        this.toStop = true;
    }

    public boolean isRunning() {
        return running;
    }

    @Override
    public void run() {

        while (!toStop) {
            try {

                // ------- url ----------
                running = false;
                crawler.tryFinish();
                String link = crawler.getRunData().getUrl();
                running = true;
                logger.info(">>>>>>>>>>> xxl crawler, process link : {}", link);
                if (!UrlUtil.isUrl(link)) {
                    continue;
                }

                // failover
                for (int i = 0; i < (1 + crawler.getRunConf().getFailRetryCount()); i++) {

                    boolean ret = false;
                    try {
                        // make request
                        PageRequest pageRequest = makePageRequest(link);

                        // pre parse
                        crawler.getRunConf().getPageParser().preParse(pageRequest);

                        // parse
                        if (crawler.getRunConf().getPageParser() instanceof NonPageParser) {
                            ret = processNonPage(pageRequest);
                        } else {
                            ret = processPage(pageRequest);
                        }
                    } catch (Throwable e) {
                        logger.info(">>>>>>>>>>> xxl crawler proocess error.", e);
                    }

                    if (crawler.getRunConf().getPauseMillis() > 0) {
                        try {
                            TimeUnit.MILLISECONDS.sleep(crawler.getRunConf().getPauseMillis());
                        } catch (InterruptedException e) {
                            logger.info(">>>>>>>>>>> xxl crawler thread is interrupted. 2{}", e.getMessage());
                        }
                    }
                    if (ret) {
                        break;
                    }
                }

            } catch (Throwable e) {
                if (e instanceof InterruptedException) {
                    logger.info(">>>>>>>>>>> xxl crawler thread is interrupted. {}", e.getMessage());
                } else if (e instanceof XxlCrawlerException) {
                    logger.info(">>>>>>>>>>> xxl crawler thread {}", e.getMessage());
                } else {
                    logger.error(e.getMessage(), e);
                }
            }

        }
    }

    /**
     * make page request
     *
     * @param link
     * @return PageRequest
     */
    private PageRequest makePageRequest(String link) {
        String userAgent = crawler.getRunConf().getUserAgentList().size() > 1
                ? crawler.getRunConf().getUserAgentList().get(new Random().nextInt(crawler.getRunConf().getUserAgentList().size()))
                : crawler.getRunConf().getUserAgentList().size() == 1 ? crawler.getRunConf().getUserAgentList().get(0) : null;
        Proxy proxy = null;
        if (crawler.getRunConf().getProxyMaker() != null) {
            proxy = crawler.getRunConf().getProxyMaker().make();
        }

        PageRequest pageRequest = new PageRequest();
        pageRequest.setUrl(link);
        pageRequest.setParamMap(crawler.getRunConf().getParamMap());
        pageRequest.setCookieMap(crawler.getRunConf().getCookieMap());
        pageRequest.setHeaderMap(crawler.getRunConf().getHeaderMap());
        pageRequest.setUserAgent(userAgent);
        pageRequest.setReferrer(crawler.getRunConf().getReferrer());
        pageRequest.setIfPost(crawler.getRunConf().isIfPost());
        pageRequest.setTimeoutMillis(crawler.getRunConf().getTimeoutMillis());
        pageRequest.setProxy(proxy);
        pageRequest.setValidateTLSCertificates(crawler.getRunConf().isValidateTLSCertificates());

        return pageRequest;
    }

    /**
     * process non page
     *
     * @param pageRequest
     * @return boolean
     */
    private boolean processNonPage(PageRequest pageRequest) {
        NonPageParser nonPageParser = (NonPageParser) crawler.getRunConf().getPageParser();

        String pagesource = JsoupUtil.loadPageSource(pageRequest);
        if (pagesource == null) {
            return false;
        }
        nonPageParser.parse(pageRequest.getUrl(), pagesource);
        return true;
    }

    /**
     * process page
     *
     * @param pageRequest
     * @return boolean
     */
    private boolean processPage(PageRequest pageRequest) throws IllegalAccessException, InstantiationException {
        Document html = crawler.getRunConf().getPageLoader().load(pageRequest);

        if (html == null) {
            return false;
        }

        // ------- child link list (FIFO队列,广度优先) ----------
        if (crawler.getRunConf().isAllowSpread()) {     // limit child spread
            Set<String> links = JsoupUtil.findLinks(html);
            if (links != null && links.size() > 0) {
                for (String item : links) {
                    if (crawler.getRunConf().validWhiteUrl(item)) {      // limit unvalid-child spread
                        crawler.getRunData().addUrl(item);
                    }
                }
            }
        }

        // ------- pagevo ----------
        if (!crawler.getRunConf().validWhiteUrl(pageRequest.getUrl())) {     // limit unvalid-page parse, only allow spread child, finish here
            return true;
        }

        // pagevo class-field info
        Class pageVoClassType = Object.class;

        Type pageVoParserClass = crawler.getRunConf().getPageParser().getClass().getGenericSuperclass();
        if (pageVoParserClass instanceof ParameterizedType) {
            Type[] pageVoClassTypes = ((ParameterizedType) pageVoParserClass).getActualTypeArguments();
            pageVoClassType = (Class) pageVoClassTypes[0];
        }

        PageSelect pageVoSelect = (PageSelect) pageVoClassType.getAnnotation(PageSelect.class);
        String pageVoCssQuery = (pageVoSelect != null && pageVoSelect.cssQuery() != null && pageVoSelect.cssQuery().trim().length() > 0) ? pageVoSelect.cssQuery() : "html";

        // pagevo document 2 object
        Elements pageVoElements = html.select(pageVoCssQuery);

        if (pageVoElements != null && pageVoElements.hasText()) {
            for (Element pageVoElement : pageVoElements) {

                Object pageVo = pageVoClassType.newInstance();

                Field[] fields = pageVoClassType.getDeclaredFields();
                if (fields != null) {
                    for (Field field : fields) {
                        if (Modifier.isStatic(field.getModifiers())) {
                            continue;
                        }


                        // field origin value
                        PageFieldSelect fieldSelect = field.getAnnotation(PageFieldSelect.class);
                        String cssQuery = null;
                        XxlCrawlerConf.SelectType selectType = null;
                        String selectVal = null;
                        if (fieldSelect != null) {
                            cssQuery = fieldSelect.cssQuery();
                            selectType = fieldSelect.selectType();
                            selectVal = fieldSelect.selectVal();
                        }
                        if (cssQuery == null || cssQuery.trim().length() == 0) {
                            continue;
                        }

                        // field value
                        Object fieldValue = null;
                        Elements elements = pageVoElement.select(cssQuery);
                        if (elements == null || elements.size() <= 0) {
                            continue;
                        }

                        if (field.getGenericType() instanceof ParameterizedType) {
                            ParameterizedType fieldGenericType = (ParameterizedType) field.getGenericType();
                            if (fieldGenericType.getRawType().equals(List.class)) {
                                //new List
                                List<Object> fieldValueTmp = new ArrayList<Object>();
                                Type gtATA = fieldGenericType.getActualTypeArguments()[0];
                                if (!gtATA.getTypeName().equals("java.lang.String")) {
                                    for (Element fieldElement : elements) {
                                        Object obj = reflectObject(fieldElement, gtATA);
                                        fieldValueTmp.add(obj);
                                    }
                                } else {
                                    //字符串形式存在
//                                    Elements fieldElementList = pageVoElement.select(cssQuery);
                                    for (Element fieldElement : elements) {

                                        String fieldElementOrigin = JsoupUtil.parseElement(fieldElement, selectType, selectVal);
                                        if (fieldElementOrigin == null || fieldElementOrigin.length() == 0) {
                                            continue;
                                        }
                                        try {
                                            fieldValueTmp.add(FieldReflectionUtil.parseValue(field, fieldElementOrigin));
                                        } catch (Exception e) {
                                            logger.error(e.getMessage(), e);
                                        }
                                    }
                                }

                                if (fieldValueTmp.size() > 0) {
                                    fieldValue = fieldValueTmp;
                                }
                            }
                        } else {

                            GetFieldVal getFieldVal = new GetFieldVal(pageVoElement, field, cssQuery, selectType, selectVal, fieldValue).invoke();
                            if (getFieldVal.is())
                                continue;
                            fieldValue = getFieldVal.getChild_fieldValue();

//                            Elements fieldElements = pageVoElement.select(cssQuery);
//                            String fieldValueOrigin = null;
//                            if (fieldElements != null && fieldElements.size() > 0) {
//                                fieldValueOrigin = JsoupUtil.parseElement(fieldElements.get(0), selectType, selectVal);
//                            }
//
//                            if (fieldValueOrigin == null || fieldValueOrigin.length() == 0) {
//                                continue;
//                            }
//
//                            try {
//                                fieldValue = FieldReflectionUtil.parseValue(field, fieldValueOrigin);
//                            } catch (Exception e) {
//                                logger.error(e.getMessage(), e);
//                            }
                        }

                        if (fieldValue != null) {
                            /*PropertyDescriptor pd = new PropertyDescriptor(field.getName(), pageVoClassType);
                            Method method = pd.getWriteMethod();
                            method.invoke(pageVo, fieldValue);*/

                            field.setAccessible(true);
                            field.set(pageVo, fieldValue);
                        }
                    }
                }

                // pagevo output
                crawler.getRunConf().getPageParser().parse(html, pageVoElement, pageVo);
            }
        }

        return true;
    }

    private Object reflectObject(Element fieldElement, Type gtATA) throws InstantiationException, IllegalAccessException {
        //对象属性
        //对 List 进行解析
//                                    Class child_class = gtATA.getClass();
        Class child_class = null;
        try {
            child_class = Class.forName(gtATA.getTypeName());
        } catch (ClassNotFoundException e) {
            logger.error("", e);
        }
        //初始化对象
        Object child_pageVo = child_class.newInstance();
        //获取对象所有属性
        Field child_fields[] = child_class.getDeclaredFields();
        if (child_fields.length > 0) {
            for (Field child_field : child_fields) {
                PageFieldSelect child_fieldSelect = child_field.getAnnotation(PageFieldSelect.class);
                String child_cssQuery = child_fieldSelect.cssQuery();
                XxlCrawlerConf.SelectType child_selectType = child_fieldSelect.selectType();
                String child_selectVal = child_fieldSelect.selectVal();
                Object child_fieldValue = null;

                GetFieldVal getFieldVal = new GetFieldVal(fieldElement, child_field, child_cssQuery, child_selectType, child_selectVal, child_fieldValue).invoke();
                if (getFieldVal.is())
                    continue;
                child_fieldValue = getFieldVal.getChild_fieldValue();

                if (child_fieldValue != null) {
                    child_field.setAccessible(true);
                    child_field.set(child_pageVo, child_fieldValue);
                }
            }
        }
        return child_pageVo;
    }



    private class GetFieldVal {
        private boolean myResult;
        private Element fieldElement;
        private Field child_field;
        private String child_cssQuery;
        private XxlCrawlerConf.SelectType child_selectType;
        private String child_selectVal;
        private Object child_fieldValue;

        public GetFieldVal(Element fieldElement, Field child_field, String child_cssQuery, XxlCrawlerConf.SelectType child_selectType, String child_selectVal, Object child_fieldValue) {
            this.fieldElement = fieldElement;
            this.child_field = child_field;
            this.child_cssQuery = child_cssQuery;
            this.child_selectType = child_selectType;
            this.child_selectVal = child_selectVal;
            this.child_fieldValue = child_fieldValue;
        }

        boolean is() {
            return myResult;
        }

        public Object getChild_fieldValue() {
            return child_fieldValue;
        }

        public GetFieldVal invoke() {
            Elements fieldElements = fieldElement.select(child_cssQuery);
            String fieldValueOrigin = null;
            if (fieldElements != null && fieldElements.size() > 0) {
                fieldValueOrigin = JsoupUtil.parseElement(fieldElements.get(0), child_selectType, child_selectVal);
            }

            if (fieldValueOrigin == null || fieldValueOrigin.length() == 0) {
                myResult = true;
                return this;
            }

            try {
                child_fieldValue = FieldReflectionUtil.parseValue(child_field, fieldValueOrigin);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
            myResult = false;
            return this;
        }
    }
}