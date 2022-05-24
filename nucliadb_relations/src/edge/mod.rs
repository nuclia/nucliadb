use std::fmt::Display;

use crate::identifier::*;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Edge {
    Child(ResourceID, ResourceID),
    About(ResourceID, LabelID),
    Entity(ResourceID, EntityID),
    Colab(ResourceID, ColaboratorID),
}
impl Display for Edge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Edge::*;
        let edge = match self {
            Child(from, to) => format!("[{}/CHILD/{}]", from, to),
            About(from, to) => format!("[{}/ABOUT/{}]", from, to),
            Entity(from, to) => format!("[{}/ENTITY/{}]", from, to),
            Colab(from, to) => format!("[{}/COLAB/{}]", from, to),
        };
        write!(f, "{}", edge)
    }
}

impl From<&str> for Edge {
    fn from(raw: &str) -> Self {
        use Edge::*;
        let raw = raw.strip_prefix('[').unwrap().strip_suffix(']').unwrap();
        let parts: Vec<&str> = raw.split('/').collect();
        let from = parts[0];
        let to = parts[2];
        match parts[1] {
            "CHILD" => Child(ResourceID::from(from), ResourceID::from(to)),
            "ABOUT" => About(ResourceID::from(from), LabelID::from(to)),
            "ENTITY" => Entity(ResourceID::from(from), EntityID::from(to)),
            "COLAB" => Colab(ResourceID::from(from), ColaboratorID::from(to)),
            _ => panic!("Unknown edge"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Query {
    ChildQ(ResourceID, Option<ResourceID>),
    AboutQ(ResourceID, Option<LabelID>),
    EntityQ(ResourceID, Option<EntityID>),
    ColabQ(ResourceID, Option<ColaboratorID>),
    AllR(ResourceID),
    AllL(LabelID),
    AllE(EntityID),
    AllC(ColaboratorID),
}

impl Display for Query {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Query::*;
        fn empty_or_encoded<T>(id: Option<ID<T>>) -> String {
            id.map_or_else(|| "".to_string(), |v| format!("{}]", v))
        }
        let edge = match self {
            ChildQ(from, to) => format!("[{}/CHILD/{}", from, empty_or_encoded(*to)),
            AboutQ(from, to) => format!("[{}/ABOUT/{}", from, empty_or_encoded(*to)),
            EntityQ(from, to) => format!("[{}/ENTITY/{}", from, empty_or_encoded(*to)),
            ColabQ(from, to) => format!("[{}/COLAB/{}", from, empty_or_encoded(*to)),
            AllR(from) => format!("[{}/", from),
            AllL(from) => format!("[{}/", from),
            AllE(from) => format!("[{}/", from),
            AllC(from) => format!("[{}/", from),
        };
        write!(f, "{}", edge)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn from_to_string() {
        let mut resource = ResourceID::new();
        let mut label = LabelID::new();
        let mut entity = EntityID::new();
        let mut colaborator = ColaboratorID::new();

        let (from, to) = (resource.next(), resource.next());
        let child_edge = Edge::Child(from, to);
        let child_query = Query::ChildQ(from, Some(to));

        let (from, to) = (resource.next(), label.next());
        let about_edge = Edge::About(from, to);
        let about_query = Query::AboutQ(from, Some(to));

        let (from, to) = (resource.next(), entity.next());
        let entity_edge = Edge::Entity(from, to);
        let entity_query = Query::EntityQ(from, Some(to));

        let (from, to) = (resource.next(), colaborator.next());
        let colab_edge = Edge::Colab(from, to);
        let colab_query = Query::ColabQ(from, Some(to));

        assert_eq!(child_edge, Edge::from(child_edge.to_string().as_str()));
        assert_eq!(child_edge, Edge::from(child_query.to_string().as_str()));
        assert_eq!(about_edge, Edge::from(about_edge.to_string().as_str()));
        assert_eq!(about_edge, Edge::from(about_query.to_string().as_str()));
        assert_eq!(entity_edge, Edge::from(entity_edge.to_string().as_str()));
        assert_eq!(entity_edge, Edge::from(entity_query.to_string().as_str()));
        assert_eq!(colab_edge, Edge::from(colab_edge.to_string().as_str()));
        assert_eq!(colab_edge, Edge::from(colab_query.to_string().as_str()));
    }
}
