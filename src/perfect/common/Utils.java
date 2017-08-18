package perfect.common;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import perfect.db.XError;

import javax.xml.parsers.DocumentBuilderFactory;
import java.lang.reflect.Field;

/**
 * Created by HuangQiang on 2017/4/21.
 */
public class Utils {

    public static String getTextContent(Element ele) {
        return ele.getFirstChild().getTextContent();
    }
    public static String getString(Element self, String name, String def) {
        String s = self.getAttribute(name);
        return s.isEmpty() ? def : s;
    }

    public static String getString(Element self, String name) {
        return self.getAttribute(name);
    }

    public static int getAttrInt(Element self, String name, int def) {
        String s = self.getAttribute(name);
        return s.isEmpty() ? def : Integer.parseInt(s);
    }

    public static int getAttrInt(Element self, String name) {
        String s = self.getAttribute(name);
        if(s.isEmpty()) throw new XError("attr:" + name + " missing");
        return Integer.parseInt(s);
    }

    public static boolean getAttrBoolean(Element self, String name, boolean def) {
        String s = self.getAttribute(name);
        return s.isEmpty() ? def : Boolean.parseBoolean(s);
    }

    public static Element openXml(String xmlFile) throws Exception {
        return DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(xmlFile).getDocumentElement();
    }

    public interface Visitor {
        void onTag(String tagName, Element data);
    }
    public static void foreach(Element data, Visitor visitor)  {
        final NodeList nodes = data.getChildNodes();
        for(int i = 0, n = nodes.getLength(); i < n ; i++) {
            final Node node = nodes.item(i);
            if (Node.ELEMENT_NODE != node.getNodeType()) continue;
            Element ele = (Element) node;
            visitor.onTag(ele.getNodeName(), ele);
        }
    }

    public static void readBean(Object bean, Element ele) {
        foreach(ele, (tag, data) -> {
            Class<?> clazz = bean.getClass();
            String content = getTextContent(data);
            try {
                Field field = clazz.getDeclaredField(tag);
                if(field == null)
                    throw new RuntimeException("tag:" + tag + " not bean:" + clazz.getName() + "'s field");
                field.setAccessible(true);
                Class<?> ftype = field.getType();
                if(ftype == int.class) {
                    field.setInt(bean, Integer.parseInt(content));
                } else if(ftype == String.class) {
                    field.set(bean, content);
                } else if(ftype == boolean.class) {
                    field.setBoolean(bean, Boolean.parseBoolean(content));
                } else if(ftype == long.class) {
                    field.setLong(bean, Long.parseLong(content));
                } else if(ftype == short.class) {
                    field.setShort(bean, Short.parseShort(content));
                } else {
                    Trace.log.debug("bean:{} unrecoginze field:{} type:{}", clazz, tag, ftype);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
