# OrientDB Import / Export

This is just a kind of playground for what will go into OrientDB tests.

**Goals:**

- Performance assessment (current state, considering empty / baseline and public databases)
- Speedup (refactoring)
- Compatibility (between ODB versions)
- Stability

**Setup:**

Build with maven:
```
mvn clean install
```

> Note: there is no API for importing database directories (i.e. manual work by copying the directories into ODB's `/databases` directory). Normal JSON files can also not be loaded. Only *special* JSON files can be imported.

**Links:**

- OrientDB export, import: http://orientdb.com/docs/3.1.x/admin/Export-to-and-Import-from-JSON.html
- Public databases from http://www.orientdb.com/public-databases/3.0.x/config.json
- Java + Docker: https://github.com/docker-java/docker-java
