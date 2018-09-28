#!/usr/bin/env groovy

// Global scope required for multi-stage persistence
def artServer = Artifactory.server 'art-p-01'
def buildInfo = Artifactory.newBuildInfo()
def distDir = 'build/dist/'
def agentSbtVersion = 'sbt_0-13-13'

pipeline {
    libraries {
        lib('jenkins-pipeline-shared')
    }
     environment {
        SVC_NAME = "sbr-enterprise-assembler"
        ORG = "SBR"
    }
    options {
        skipDefaultCheckout()
        buildDiscarder(logRotator(numToKeepStr: '30', artifactNumToKeepStr: '30'))
        timeout(time: 1, unit: 'HOURS')
        ansiColor('xterm')
    }
    agent { label 'download.jenkins.slave' }
    stages {
        stage('Checkout') {
            agent { label 'download.jenkins.slave' }
            steps {
                checkout scm        
                script {
                    buildInfo.name = "${SVC_NAME}"
                    buildInfo.number = "${BUILD_NUMBER}"
                    buildInfo.env.collect()
                }
                colourText("info", "BuildInfo: ${buildInfo.name}-${buildInfo.number}")
                stash name: 'Checkout'
            }
        }

        stage('Build'){
            agent { label "build.${agentSbtVersion}" }
            steps {
                unstash name: 'Checkout'
                sh "sbt compile"
            }
            post {
                success {
                    postSuccess()
                }
                failure {
                    postFail()
                }
            }
        }

        stage('Validate') {
            failFast true
            parallel {
                stage('Test: Unit'){
                    agent { label "build.${agentSbtVersion}" }
                    steps {
                        unstash name: 'Checkout'
                        sh 'sbt coverage test coverageReport coverageAggregate'
                    }
                    post {
                        always {
                            junit allowEmptyResults: true, testResults: 'target/test-reports/*.xml'
                            cobertura autoUpdateHealth: false, 
                                autoUpdateStability: false, 
                                coberturaReportFile: 'target/**/coverage-report/cobertura.xml', 
                                conditionalCoverageTargets: '70, 0, 0', 
                                failUnhealthy: false, 
                                failUnstable: false, 
                                lineCoverageTargets: '80, 0, 0', 
                                maxNumberOfBuilds: 0, 
                                methodCoverageTargets: '80, 0, 0', 
                                onlyStable: false, 
                                zoomCoverageChart: false
                        }
                        success {
                            postSuccess()
                        }
                        failure {
                            postFail()
                        }
                    }
                }
                stage('Style') {
                    agent { label "build.${agentSbtVersion}" }
                    steps {
                        unstash name: 'Checkout'
                        colourText("info","Running style tests")
                        sh 'sbt scalastyleGenerateConfig scalastyle'
                    }
                    post {
                        always {
                            checkstyle canComputeNew: false, defaultEncoding: '', healthy: '', pattern: 'target/scalastyle-result.xml', unHealthy: ''   
                        }
                    }
                }
            }
            post {
                success {
                    postSuccess()
                }
                failure {
                    postFail()
                }
            }
        }

        stage('Test: Acceptance') {
            agent { label "build.${agentSbtVersion}" }
            steps {
                unstash name: 'Checkout'
                sh "sbt it:test"
            }
            post {
                always {
                    junit '**/target/test-reports/*.xml'
                }
                success {
                    postSuccess()
                }
                failure {
                    postFail()
                }
            }
        }

        stage ('Publish') {
            agent { label "build.${agentSbtVersion}" }
            when { 
                branch "master" 
                // evaluate the when condition before entering this stage's agent, if any
                beforeAgent true 
            }
            steps {
                colourText("info", "Building ${env.BUILD_ID} on ${env.JENKINS_URL} from branch ${env.BRANCH_NAME}")
                unstash name: 'Checkout'
                sh 'sbt assembly'
                script {
                    def uploadSpec = """{
                        "files": [
                            {
                                "pattern": "target/universal/*.zip",
                                "target": "registers-sbt-snapshots/uk/gov/ons/${buildInfo.name}/${buildInfo.number}/"
                            }
                        ]
                    }"""
                    artServer.upload spec: uploadSpec, buildInfo: buildInfo
                }
            }
            post {
                success {
                    postSuccess()
                }
                failure {
                    postFail()
                }
            }
        }

        stage('Deploy: Dev'){
            agent { label 'deploy.cf' }
            when { 
                branch "master"
                // evaluate the when condition before entering this stage's agent, if any
                beforeAgent true 
            }
            environment{
                CREDS = 's_jenkins_sbr_dev'
                SPACE = 'Dev'
            }
            steps {
                script {
                    def downloadSpec = """{
                        "files": [
                            {
                                "pattern": "registers-sbt-snapshots/uk/gov/ons/${buildInfo.name}/${buildInfo.number}/*.zip",
                                "target": "${distDir}",
                                "flat": "true"
                            }
                        ]
                    }"""
                    artServer.download spec: downloadSpec, buildInfo: buildInfo
                    sh "mv ${distDir}*.zip ${distDir}${env.SVC_NAME}.zip"
                }
                // TODO: Deploy to HBase
            }
            post {
                success {
                    postSuccess()
                }
                failure {
                    postFail()
                }
            }
        }
    }

    post {
        success {
            colourText("success", "All stages complete. Build was successful.")
            slackSend(
                color: "good",
                message: "${env.JOB_NAME} success: ${env.RUN_DISPLAY_URL}"
            )
        }
        unstable {
            colourText("warn", "Something went wrong, build finished with result ${currentResult}. This may be caused by failed tests, code violation or in some cases unexpected interrupt.")
            slackSend(
                color: "warning",
                message: "${env.JOB_NAME} unstable: ${env.RUN_DISPLAY_URL}"
            )
        }
        failure {
            colourText("warn","Process failed at: ${env.NODE_STAGE}")
            slackSend(
                color: "danger",
                message: "${env.JOB_NAME} failed at ${env.STAGE_NAME}: ${env.RUN_DISPLAY_URL}"
            )
        }
    }
}

def postSuccess() {
    colourText('info', "Stage: ${env.STAGE_NAME} successfull!")
}

def postFail() {
    colourText('warn', "Stage: ${env.STAGE_NAME} failed!")
}