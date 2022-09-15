use unidecode::unidecode;

pub fn normalize(value: &str) -> String {
    unidecode(&value.to_lowercase())
}
