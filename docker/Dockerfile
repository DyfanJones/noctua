FROM rocker/r-base

RUN apt-get update
RUN apt-get install -y --no-install-recommends \
    curl \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev

# get aws cli
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install

# Install noctua
RUN Rscript -e "install.packages(c('paws', 'noctua'), repos = c(pawsr = 'https://paws-r-builds.s3.amazonaws.com/packages/latest/', CRAN = 'https://cloud.r-project.org'))"

CMD ["R"]
