use crate::misc::escape_xml;

pub trait FormatXML {
    fn to_xml_string(&self, key: &str) -> String;
}

impl FormatXML for String {
    fn to_xml_string(&self, key: &str) -> String {
        format!("<{0}>{1}</{0}>", key, escape_xml(&self))
    }
}

impl<T> FormatXML for Vec<T>
where
    T: FormatXML,
{
    fn to_xml_string(&self, key: &str) -> String {
        let list: Vec<String> = self.iter().map(|v| v.to_xml_string(key)).collect();
        list.join("")
    }
}
