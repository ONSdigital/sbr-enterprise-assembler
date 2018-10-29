#!groovy
@Library('jenkins-pipeline-shared') _

pipeline {
    environment {
        RELEASE_TYPE = "PATCH"

        BRANCH_DEV = "develop"
        BRANCH_TEST = "release"
        BRANCH_PROD = "master"

        DEPLOY_DEV = "dev"
        DEPLOY_TEST = "test"
        DEPLOY_PROD = "prod"

        CF_CREDS = "sbr-api-dev-secret-key"

        GIT_TYPE = "Github"
        GIT_CREDS = "github-sbr-user"
        GITLAB_CREDS = "sbr-gitlab-id"

        ORGANIZATION = "ons"
        TEAM = "sbr"
        MODULE_NAME = "sbr-enterprise-assembler"

        // hbase config
        CH_TABLE = "ch"
        VAT_TABLE = "vat"
        PAYE_TABLE = "paye"
        NAMESPACE = "sbr_dev_db"
    }
    options {
        skipDefaultCheckout()
        buildDiscarder(logRotator(numToKeepStr: '30', artifactNumToKeepStr: '30'))
        timeout(time: 30, unit: 'MINUTES')
        timestamps()
    }
    agent any
    stages {
        stage('Checkout') {
            agent any
            steps {
                deleteDir()
                checkout scm
                stash name: 'app'
                sh "$SBT version"
                script {
                    version = '1.0.' + env.BUILD_NUMBER
                    currentBuild.displayName = version
                    env.NODE_STAGE = "Checkout"
                }
            }
        }

        stage ('Package and Push Artifact') {
            agent any
           /* when {
                anyOf {
                    branch DEPLOY_DEV
                    branch DEPLOY_TEST
                }
            }*/
            steps {
                script {
                    env.NODE_STAGE = "Package and Push Artifact"
                }
                sh """
		    mkdir $WORKSPACE/tmp
                   # $SBT -Djava.io.tmpdir=$WORKSPACE/tmp clean compile assembly
		   $SBT 'set test in assembly := {}' clean compile assembly
                """
                copyToHBaseNode()
                colourText("success", 'Package.')
            }
        }


    }
}

def push (String newTag, String currentTag) {
    echo "Pushing tag ${newTag} to Gitlab"
    GitRelease( GIT_CREDS, newTag, currentTag, "${env.BUILD_ID}", "${env.BRANCH_NAME}", GIT_TYPE)
}

def deploy (String dataSource) {
    CF_SPACE = "${env.DEPLOY_NAME}".capitalize()
    CF_ORG = "${TEAM}".toUpperCase()
    echo "Deploying Api app to ${env.DEPLOY_NAME}"
    withCredentials([string(credentialsId: CF_CREDS, variable: 'APPLICATION_SECRET')]) {
        deployToCloudFoundryHBase("${TEAM}-${env.DEPLOY_NAME}-cf", "${CF_ORG}", "${CF_SPACE}", "${env.DEPLOY_NAME}-${dataSource}-${MODULE_NAME}", "${env.DEPLOY_NAME}-${ORGANIZATION}-${MODULE_NAME}.zip", "gitlab/${env.DEPLOY_NAME}/manifest.yml", "${dataSource}", NAMESPACE)
    }
}

def copyToHBaseNode() {
    echo "Deploying to $DEPLOY_DEV"
    sshagent(credentials: ["sbr-$DEPLOY_DEV-ci-ssh-key"]) {
        withCredentials([string(credentialsId: "sbr-hbase-node", variable: 'HBASE_NODE')]) {
            sh '''
                ssh sbr-$DEPLOY_DEV-ci@$HBASE_NODE mkdir -p $MODULE_NAME/lib
                scp ${WORKSPACE}/target/scala-2.11/sbr-enterprise-assembler-assembly*.jar sbr-$DEPLOY_DEV-ci@$HBASE_NODE:$MODULE_NAME/lib/
                echo "Successfully copied jar file to $MODULE_NAME/lib directory on $HBASE_NODE"
                ssh sbr-$DEPLOY_DEV-ci@$HBASE_NODE hdfs dfs -put -f $MODULE_NAME/lib/sbr-enterprise-assembler-assembly*.jar hdfs://prod1/user/sbr-$DEPLOY_DEV-ci/lib/
                echo "Successfully copied jar file to HDFS"
	        '''
        }
    }
}
