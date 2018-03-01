# The FROM instruction sets the Base Image for subsequent instructions.
# Using Nginx as Base Image
FROM daocloud.io/idatage/ubuntu-py3:latest
MAINTAINER xshell <fsjyh1988@163.com>

# The RUN instruction will execute any commands
# Adding HelloWorld page into Nginx server
RUN echo "Asia/Shanghai" > /etc/timezone && dpkg-reconfigure -f noninteractive tzdata
RUN mkdir /working
COPY ./ /working
WORKDIR /working
