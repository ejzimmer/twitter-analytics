const fs = require('fs');
const parse = require('csv-parse');
const transform = require('stream-transform')

const hashTags = {};
const posts = [];

const topHashTags = (measure, count = 10) => Object.entries(hashTags)
                                          .sort(([tagA, dataA], [tagB, dataB]) => dataB[measure]/dataB.tweets.length - dataA[measure]/dataA.tweets.length)
                                          .slice(0, count);

const topHashTagRates = (measure, count = 10) => {
  Object.entries(hashTags)
    .map(([tag, metadata]) => [tag, metadata[measure] / metadata.impressions])
    .sort(([tagA, rateA], [tagB, rateB]) => rateB - rateA)
    .slice(0, count)
}

const getHashTags = (tweet) => {
  const tags = tweet['Tweet text'].split(' ').filter(word => word.startsWith('#'));
  const { impressions, engagements } = tweet;
  const clicks = tweet['url clicks'];

  tags.forEach(tag => {

    const sanitisedTag = tag.toLowerCase().replace(/[^a-z#]/g, '');
    if (!hashTags[sanitisedTag]) {
      hashTags[sanitisedTag] = {
        tag: sanitisedTag,
        impressions,
        engagements,
        clicks, 
        tweets: [tweet['Tweet permalink']],
      } 
    } else {
      hashTags[sanitisedTag].impressions += impressions;
      hashTags[sanitisedTag].engagements += engagements;
      hashTags[sanitisedTag].clicks += clicks;
      hashTags[sanitisedTag].tweets.push(tweet['Tweet permalink']);
    }
  })
}

const topPosts = (measure, count = 10) => posts
                                            .sort((postA, postB) => postB[measure] - postA[measure])
                                            .map(post => ({ id: post['Tweet id'], text: post['Tweet text'], [measure]: post[measure] }))
                                            .slice(0, count);

if (fs.existsSync('all_tweets.csv'))
  fs.unlinkSync('all_tweets.csv');
const csvFiles = fs.readdirSync('.');
const allTweets = [];
let columns;
csvFiles
  .filter(fileName => fileName.endsWith('.csv'))
  .forEach(fileName => {
    const buffer = fs.readFileSync(fileName, 'utf8');
    const lines = buffer.split('\n');
    columns = lines[0];
    lines.splice(0, 1);
    allTweets.push(lines.join('\n'));
  });
allTweets.unshift(columns);
fs.writeFileSync('all_tweets.csv', allTweets.join('\n'));


const stream = fs.createReadStream('all_tweets.csv')
  .pipe(parse({ columns: true, auto_parse: true }))
  .pipe(transform((tweet) => {
    if (tweet['Tweet id']) {
      posts.push(tweet);
      getHashTags(tweet);
    }
  }));

const printHashTag = (tag, measure) => `${tag[0]}: ${tag[1][measure]/tag[1].tweets.length} ${tag[1].tweets.join(', ')}`;

stream.on('finish', () => {
  const topTenImpressions = topPosts('impressions');
  const topTenEngagements = topPosts('engagements');
  const topTenClicks = topPosts('url clicks');

  const lines = [];
  lines.push('Top 10 Impressions');
  lines.push(...topTenImpressions.map(tweet => tweet.text));
  lines.push('\n');
  lines.push('Top ten engagements');
  lines.push(...topTenEngagements.map(tweet => tweet.text));
  lines.push('\n');
  lines.push('Top ten clicks');
  lines.push(...topTenClicks.map(tweet => tweet.text));

  const topHashtagImpressions = topHashTags('impressions');
  const topHashtagEngagements = topHashTags('engagements');
  const topHashTagClicks = topHashTags('clicks');

  lines.push('\n\nTop hashtag impressions');
  lines.push(...topHashtagImpressions.map(tag => printHashTag(tag, 'impressions')));
  lines.push('\n');
  lines.push('Top hashtag engagements');
  lines.push(...topHashtagEngagements.map(tag => printHashTag(tag, 'engagements')));
  lines.push('\n');
  lines.push('Top hashtag clicks');
  lines.push(...topHashTagClicks.map(tag => printHashTag(tag, 'clicks')));

  fs.writeFileSync('top_ten_tweets', lines.join('\n'));  
});
