# What does this do?

This program creates SSTables given the cassandra schema definition and the data as a CSV file. This program is especially important when uploading data to a cassandra instance when the line count is larger than 2 million.

# How to use this?

This example program creates SSTables for a specific column family.

   * Copy the CSV data dump to the input folder.
   * Copy the CQL scolumn family schema definition containing .cql file to resources folder.

Create the artefacts using Maven.
```sh
    $ cd CSVtoSSTable
    $ mvn clean install
```
Use the jar file in the target directory as a library.
