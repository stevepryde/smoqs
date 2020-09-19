use indexmap::IndexMap;

pub trait FormatXML {
    fn to_xml_string(&self, key: &str) -> String;
}

impl FormatXML for String {
    fn to_xml_string(&self, key: &str) -> String {
        format!("<{0}>{1}</{0}>", key, self)
    }
}

impl<T> FormatXML for IndexMap<String, T>
where
    T: FormatXML,
{
    fn to_xml_string(&self, key: &str) -> String {
        let content: Vec<String> = self.iter().map(|(k, v)| v.to_xml_string(k)).collect();
        format!("<{0}>{1}</{0}>", key, content.join(""))
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
