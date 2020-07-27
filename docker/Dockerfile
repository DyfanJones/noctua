# This example uses rstudio r-base docker images: https://github.com/rstudio/r-docker
FROM rstudio/r-base:4.0.2-centos7

# Set up centos
RUN sudo yum install -y https://repo.ius.io/ius-release-el7.rpm \
   && sudo yum update -y \
   && sudo yum install -y libcurl-devel libxml2-devel openssl-devel
   
# Set up docker with aws credentials
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && sudo ./aws/install

# Install RAthena
RUN R -e "install.packages(c('paws', 'readr', 'noctua'), repos='https://cran.rstudio.com/')"

CMD ["R"]
