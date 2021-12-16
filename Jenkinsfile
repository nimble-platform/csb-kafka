node ('nimble-jenkins-slave') {
    stage('Download Latest') {
        git(url: 'https://github.com/nimble-platform/csb-kafka.git', branch: 'master')
    }

    stage ('Build docker image') {
        sh 'mvn clean install'
        sh 'docker build -t nimbleplatform/csb-kafka:${BUILD_NUMBER} .'
        sh 'sleep 5' // For the tag to populate
    }

    stage ('Push docker image') {
        echo "docker push disabled"
#        withDockerRegistry([credentialsId: 'NimbleDocker']) {
#            sh 'docker push nimbleplatform/csb-kafka:${BUILD_NUMBER}'
#        }
    }

    stage ('Deploy') {
        sh ''' sed -i 's/IMAGE_TAG/'"$BUILD_NUMBER"'/g' kubernetes/deploy.yaml '''
        //sh 'kubectl create -f kubernetes/deploy.yaml -n prod --validate=false'
        sh 'kubectl apply -f kubernetes/deploy.yaml -n prod --validate=false'
        sh 'kubectl apply -f kubernetes/svc.yaml -n prod --validate=false'
    }

    stage ('Print-deploy logs') {
        sh 'sleep 60'
        sh 'kubectl  -n prod logs deploy/csb-kafka -c csb-kafka'
    }
}
