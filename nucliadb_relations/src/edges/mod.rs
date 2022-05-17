use crate::nodes::*;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Node {
    Child(ResourceID, ResourceID),
    About(ResourceID, LabelID),
    Entity(ResourceID, EntityID),
    Colab(ResourceID, ColaboratorID),
}
