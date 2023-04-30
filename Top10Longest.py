from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

class LongestTitlesAndBestMovies(MRJob):

    MIN_COUNT = 10
    SHOW_LIMIT = 10

    def movie_title(self, movie_id):
        '''
        Convert from movie id to movie title
        '''
        with open("/root/input/u.item", "r", encoding="ISO-8859-1") as infile:
            reader = csv.reader(infile, delimiter='|')
            next(reader)
            for line in reader:
                if int(movie_id) == int(line[0]):
                    return line[1]

    def steps(self):
        '''
        Pipeline of MapReduce tasks
        '''
        return [
            MRStep(mapper=self.mapper1, reducer=self.reducer1),
            MRStep(mapper=self.mapper2, reducer=self.reducer2),
            MRStep(mapper=self.mapper3, reducer=self.reducer3)
        ]

    def mapper1(self, _, line):
        fields = line.strip().split('\t')
        if len(fields) >= 4:
            user_id, movie_id, rating, timestamp = fields[:4]
            yield movie_id, len(movie_id), float(rating)


    def reducer1(self, movie_id, title_ratings):
        title = None
        ratings = []
        for tr in title_ratings:
            if title is None:
                title = tr[0]
            ratings.append(tr[1])
        if len(ratings) >= self.MIN_COUNT:
            yield None, (len(title), sum(ratings)/float(len(ratings)), title)

    def mapper2(self, _, value):
        yield None, value

    def reducer2(self, _, values):
        i = 0
        for _, avg_rating, title in sorted(values, reverse=True):
            i += 1
            if i <= self.SHOW_LIMIT:
                yield title, avg_rating

    def mapper3(self, title, avg_rating):
        yield None, (-len(title), title, avg_rating)

    def reducer3(self, _, values):
        i = 0
        for _, title, avg_rating in sorted(values, reverse=True):
            i += 1
            if i <= self.SHOW_LIMIT:
                yield title, avg_rating

if __name__ == '__main__':
    LongestTitlesAndBestMovies.run()
