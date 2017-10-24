# TODO

## Priority

- Enable array columns (for JSON)
- Show jCommander error message at the end of output

## Parsing

- Save all parse errors

## Formatting

- Fix formatting clash with language suffix being present already in uri e.g. /name-en = "abc" VS name = "def"@en
- Use domain names and other substrings for resolving name conflicts instead of numeric suffixes

## Instances
- Support for blank nodes
- Saving errors separately
- Merging sameAs resources, types, properties
- Detect 1:n relationships
- entities.ignoredType - types that don't create a table (but the instances are preserved if they also have an other type)

## Testing

- Formatting names from labels
- Skipping columns already stored in superclass
- Writing indexes
- Convert job with filtered type

## Persistence

- Save instance types in EAV table
- Check if table is already present and ignore creating indexes with SaveMode=Ignore to avoid log errors

## Performance

- Persist relations to single table and then move the data using SQL
- Encode instance predicate as long, use fastutil library Long2ObjectMap

## Schema

- Separate schema extraction and data load
	- all references in schema represented by URIs, not indexes
	- save entity and relation schema to xml file (owl? what about properties with multiple languages / types?)
	- xml file can be edited by hand (e.g. each relation table)
	- load instances using the schema

## README

- Sparse values example
