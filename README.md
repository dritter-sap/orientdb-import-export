# OrientDB Import / Export

This is just a kind of *playground* for what will go into OrientDB tests regarding import and maybe export.

**Goals:**

- Performance assessment (current state, considering empty / baseline and public databases)
- Memory usage (OOM)
- Speedup (refactoring)
- Compatibility (between ODB versions)
- Stability

**Setup:**

Build with maven:
```
mvn clean package
```

> Note: there is no API for importing database directories (i.e. manual work by copying the directories into ODB's `/databases` directory). Normal JSON files can also not be loaded. Only *special* JSON files can be imported.

If no local installation is at hand (also for older versions):
```
 docker run -d --name orientdb -p 2424:2424 -p 2480:2480 -e ORIENTDB_ROOT_PASSWORD=admin orientdb:2.2.27
 ```
 From the docker container, a database can be downloaded and added to another server, e.g. to test non export/import migration.

**TODOs**

- [ ] use cases
- [ ] super node test
- [ ] open CE tickets

**Links:**

- OrientDB export, import: http://orientdb.com/docs/3.1.x/admin/Export-to-and-Import-from-JSON.html
- Public databases from http://www.orientdb.com/public-databases/3.0.x/config.json
- Java + Docker: https://github.com/docker-java/docker-java
- Previous releases: https://orientdb.org/download-previous
