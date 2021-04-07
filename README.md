# Designing Data Intensive Applications

This project is a materialization of many of the concepts described in the book [Designing Data-Intensive 
Applications](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/) by
_Martin Kleppmann_.

## Project organization

The project is written in _Kotlin_ and is managed by _Graddle_.

```
Home
 |- src
 |   |- main -> implementations
 |   |- test -> instance generators and test code
```

## Testing

This project promotes a _Behavior Driven Design_ ([source](https://en.wikipedia.org/wiki/Behavior-driven_development))
approach for testing. Testing code is usually defined in interfaces or abstract classes via 
[JUnit's dynamic test](https://junit.org/junit5/docs/current/user-guide/#writing-tests-dynamic-tests). Hence, test 
instance creation is decoupled from the actual testing, allowing us to apply a given test to a group of instances.
