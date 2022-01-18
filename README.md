# spark-custom-rdd

Extending Our Spark SQL Query Engine with a custom data source as per http://blog.madhukaraphatak.com/extending-spark-api/

# How to build
- `./gradlew build`
- If you want to use locally built spark libs
    - Uncomment `flatDir` in `build.gradle`
    - Replace `<spark base path>` with locally built spark path
