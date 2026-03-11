FROM jenkins/jenkins:lts

USER root
RUN apt-get update -qq && apt-get install -y rsync && rm -rf /var/lib/apt/lists/*
USER jenkins
