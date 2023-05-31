/// metrics
/// Every metric must be define in its own module, which must fulfill the following requirements:
/// - The name of the module must be the name of the name of the metric.
/// - If the metric is called SomeName, then there must be a type 'SomeNameMetric' describing such
///   metric.
/// - If the metric is called SomeName, a function 'register_some_name' must be defined and its job
///   is to recive a registry, register there the metric and return such metric.
/// - If the metric is called SomeName, a struct 'SomeNameKey' must be defined.
/// - If the metric is called SomeName, a struct 'SomeNameValue' must be defined.
pub mod request_time;
pub mod tokio_tasks;
