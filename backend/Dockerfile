FROM maven:3.9.9 as build
WORKDIR /app
ARG CONTAINER_PORT
COPY pom.xml /app
RUN mvn dependency:resolve
COPY . /app
RUN mvn clean
RUN mvn package -DskipTests -X


FROM openjdk:22-oracle
COPY --from=build /app/target/*.jar app.jar
COPY src/main/resources /app/src/main/resources
EXPOSE ${CONTAINER_PORT}
CMD ["java","--add-opens=java.base/java.nio=ALL-UNNAMED","-jar","app.jar"]
