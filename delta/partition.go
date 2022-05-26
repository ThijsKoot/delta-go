package delta

// /// A Enum used for selecting the partition value operation when filtering a DeltaTable partition.
// pub enum PartitionValue<T> {
//     /// The partition value with the equal operator
//     Equal(T),
//     /// The partition value with the not equal operator
//     NotEqual(T),
//     /// The partition value with the greater than operator
//     GreaterThan(T),
//     /// The partition value with the greater than or equal operator
//     GreaterThanOrEqual(T),
//     /// The partition value with the less than operator
//     LessThan(T),
//     /// The partition value with the less than or equal operator
//     LessThanOrEqual(T),
//     /// The partition values with the in operator
//     In(Vec<T>),
//     /// The partition values with the not in operator
//     NotIn(Vec<T>),
// }

/// A Struct DeltaTablePartition used to represent a partition of a DeltaTable.
type DeltaTablePartition struct {
	/// The key of the DeltaTable partition.
	Key string
	/// The value of the DeltaTable partition.
	Value string
}
