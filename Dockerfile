FROM ruby:alpine
WORKDIR /cequel
# Since this is a docker container to aid with development, we simply copy the entire repo versus caching
# the gems
COPY . /cequel/
RUN apk add --update build-base postgresql-dev tzdata bash bash-completion git \
    && rm -rf /var/cache/apk/* \
    # makes it nicer to run tests.. now "ber" instead of "bundle exec rspec" and "bi" instead of "bundle install"
    && echo 'alias ber="bundle exec rspec"' >> ~/.bashrc \
    && echo 'alias bi="bundle install"' >> ~/.bashrc \
    && gem install bundler \
    && bundle install
