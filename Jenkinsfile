pipeline {
    agent { node { label 'master' } }
    tools {
        maven 'Maven 3.3.9'
        jdk 'jdk8'
    }
    stages {
        stage ('Build') {
            steps {
                sh '''
                    cd task-2
                    mvn clean package -DskipTests
                '''
            }
        }

        stage ('Test') {
            steps {
                sh 'mvn test' 
            }
            post {
                success {
                    junit 'target/surefire-reports/**/*.xml' 
                }
            }
        }
    }
}
