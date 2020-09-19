use std::collections::HashMap;

pub fn get_new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

pub fn get_attributes(form: &HashMap<String, String>) -> HashMap<String, String> {
    let mut attributes = HashMap::new();
    for count in 1..100 {
        if let Some(k) = form.get(&format!("Attribute.{}.Name", count)) {
            if let Some(v) = form.get(&format!("Attribute.{}.Value", count)) {
                attributes.insert(k.clone(), v.clone());
                continue;
            }
        }

        break;
    }
    attributes
}

pub fn get_message_attributes(form: &HashMap<String, String>) -> HashMap<String, String> {
    let mut attributes = HashMap::new();
    for count in 1..100 {
        if let Some(k) = form.get(&format!("MessageAttribute.{}.Name", count)) {
            if let Some(v) = form.get(&format!("MessageAttribute.{}.Value", count)) {
                attributes.insert(k.clone(), v.clone());
                continue;
            }
        }

        break;
    }
    attributes
}

pub fn get_message_attribute_names(form: &HashMap<String, String>) -> Vec<String> {
    let mut attribute_names = Vec::new();
    for count in 1..100 {
        if let Some(k) = form.get(&format!("MessageAttribute.{}.Name", count)) {
            attribute_names.push(k.clone());
            continue;
        }

        break;
    }
    attribute_names
}
