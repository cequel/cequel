FROM ruby:alpine

# Put the basic system setup in a layer of its own so we don't have
# rebuild it all the time.
RUN apk add --update build-base tzdata bash bash-completion git \
    && rm -rf /var/cache/apk/*

WORKDIR /cequel

# Make it nicer to run tests.. now "ber" instead of "bundle exec rspec" and "bi" instead of "bundle install"
RUN echo 'alias be="bundle exec"' >> ~/.bashrc \
    && echo 'alias ber="bundle exec rspec"' >> ~/.bashrc \
    && echo 'alias bi="bundle install"' >> ~/.bashrc

# Put the bundle in a layer of its own. The bundle doesn't change
# that often to copy just the Gemfiles and bundle to build a layer
# that rarely changes
COPY ./Gemfile* /cequel/
COPY ./cequel.gemspec /cequel/
RUN mkdir -p /cequel/lib/cequel/
COPY ./lib/cequel/version.rb /cequel/lib/cequel/
RUN gem install bundler \
    && bundle install


COPY ./* /cequel/



