// This file is @generated by prost-build.
/// Relations are connexions between nodes in the relation index.
/// They are tuplets (Source, Relation Type, Relation Label, To).
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Relation {
    #[prost(message, optional, tag = "6")]
    pub source: ::core::option::Option<RelationNode>,
    #[prost(message, optional, tag = "7")]
    pub to: ::core::option::Option<RelationNode>,
    #[prost(enumeration = "relation::RelationType", tag = "5")]
    pub relation: i32,
    #[prost(string, tag = "8")]
    pub relation_label: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "9")]
    pub metadata: ::core::option::Option<RelationMetadata>,
}
/// Nested message and enum types in `Relation`.
pub mod relation {
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration
    )]
    #[repr(i32)]
    pub enum RelationType {
        /// Child resource
        Child = 0,
        /// related with label (GENERATED)
        About = 1,
        /// related with an entity (GENERATED)
        Entity = 2,
        /// related with user (GENERATED)
        Colab = 3,
        /// Synonym relation
        Synonym = 4,
        /// related with something
        Other = 5,
    }
    impl RelationType {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                RelationType::Child => "CHILD",
                RelationType::About => "ABOUT",
                RelationType::Entity => "ENTITY",
                RelationType::Colab => "COLAB",
                RelationType::Synonym => "SYNONYM",
                RelationType::Other => "OTHER",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "CHILD" => Some(Self::Child),
                "ABOUT" => Some(Self::About),
                "ENTITY" => Some(Self::Entity),
                "COLAB" => Some(Self::Colab),
                "SYNONYM" => Some(Self::Synonym),
                "OTHER" => Some(Self::Other),
                _ => None,
            }
        }
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RelationMetadata {
    #[prost(string, optional, tag = "1")]
    pub paragraph_id: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(int32, optional, tag = "2")]
    pub source_start: ::core::option::Option<i32>,
    #[prost(int32, optional, tag = "3")]
    pub source_end: ::core::option::Option<i32>,
    #[prost(int32, optional, tag = "4")]
    pub to_start: ::core::option::Option<i32>,
    #[prost(int32, optional, tag = "5")]
    pub to_end: ::core::option::Option<i32>,
}
/// Nodes are tuplets (Value, Type, Subtype) and they are the main element in the relation index.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RelationNode {
    /// Value of the node.
    #[prost(string, tag = "4")]
    pub value: ::prost::alloc::string::String,
    /// The type of the node.
    #[prost(enumeration = "relation_node::NodeType", tag = "5")]
    pub ntype: i32,
    /// A node may have a subtype (the string should be empty in case it does not).
    #[prost(string, tag = "6")]
    pub subtype: ::prost::alloc::string::String,
}
/// Nested message and enum types in `RelationNode`.
pub mod relation_node {
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration
    )]
    #[repr(i32)]
    pub enum NodeType {
        Entity = 0,
        Label = 1,
        Resource = 2,
        User = 3,
    }
    impl NodeType {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                NodeType::Entity => "ENTITY",
                NodeType::Label => "LABEL",
                NodeType::Resource => "RESOURCE",
                NodeType::User => "USER",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "ENTITY" => Some(Self::Entity),
                "LABEL" => Some(Self::Label),
                "RESOURCE" => Some(Self::Resource),
                "USER" => Some(Self::User),
                _ => None,
            }
        }
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExtractedText {
    #[prost(string, tag = "1")]
    pub text: ::prost::alloc::string::String,
    #[prost(map = "string, string", tag = "2")]
    pub split_text: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
    #[prost(string, repeated, tag = "3")]
    pub deleted_splits: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Vector {
    #[prost(int32, tag = "1")]
    pub start: i32,
    #[prost(int32, tag = "2")]
    pub end: i32,
    #[prost(int32, tag = "3")]
    pub start_paragraph: i32,
    #[prost(int32, tag = "4")]
    pub end_paragraph: i32,
    #[prost(float, repeated, tag = "5")]
    pub vector: ::prost::alloc::vec::Vec<f32>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Vectors {
    #[prost(message, repeated, tag = "1")]
    pub vectors: ::prost::alloc::vec::Vec<Vector>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VectorObject {
    #[prost(message, optional, tag = "1")]
    pub vectors: ::core::option::Option<Vectors>,
    #[prost(map = "string, message", tag = "2")]
    pub split_vectors: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        Vectors,
    >,
    #[prost(string, repeated, tag = "3")]
    pub deleted_splits: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UserVector {
    #[prost(float, repeated, tag = "1")]
    pub vector: ::prost::alloc::vec::Vec<f32>,
    #[prost(string, repeated, tag = "2")]
    pub labels: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(int32, tag = "3")]
    pub start: i32,
    #[prost(int32, tag = "4")]
    pub end: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UserVectors {
    /// vector's id
    #[prost(map = "string, message", tag = "1")]
    pub vectors: ::std::collections::HashMap<::prost::alloc::string::String, UserVector>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UserVectorSet {
    /// vectorsets
    #[prost(map = "string, message", tag = "1")]
    pub vectors: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        UserVectors,
    >,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UserVectorsList {
    #[prost(string, repeated, tag = "1")]
    pub vectors: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Security {
    #[prost(string, repeated, tag = "1")]
    pub access_groups: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum VectorSimilarity {
    Cosine = 0,
    Dot = 1,
}
impl VectorSimilarity {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            VectorSimilarity::Cosine => "COSINE",
            VectorSimilarity::Dot => "DOT",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "COSINE" => Some(Self::Cosine),
            "DOT" => Some(Self::Dot),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ReleaseChannel {
    Stable = 0,
    Experimental = 1,
}
impl ReleaseChannel {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ReleaseChannel::Stable => "STABLE",
            ReleaseChannel::Experimental => "EXPERIMENTAL",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "STABLE" => Some(Self::Stable),
            "EXPERIMENTAL" => Some(Self::Experimental),
            _ => None,
        }
    }
}
