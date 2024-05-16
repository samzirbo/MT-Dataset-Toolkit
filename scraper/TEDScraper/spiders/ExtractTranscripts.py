import html
import scrapy
import pandas as pd
import json
import os

class ExtractTranscriptsSpider(scrapy.Spider):
    """
    Run:
        scrapy crawl ExtractTranscripts 
            [-a INPUT=data/talks.csv] [-a OUTPUT=data/transcripts.json] [-a LANGUAGES=en,fr,es] [-a MAX_RETRIES] [-a MAX_TALKS] -o/O transcripts.jsonl:jsonlines

        -a options are used to pass arguments to the spider
            INPUT: Path to the input file containing the talks
            OUTPUT: Path to the output file to save the transcripts
            LANGUAGES: Comma separated list of languages to extract the transcripts for (language_mapping.csv)
            MAX_RETRIES: Maximum number of retries to request the transcript for a talk
            MAX_TALKS: Maximum number of talks to request the transcript for

        -o option is used to save the output to a file in the specified format

    Given a dataframe of talks (with id or name), request the transcript for each talk in the required languages
    """
    name = "ExtractTranscripts"
    allowed_domains = ["ted.com"]
    start_urls = ["https://ted.com/talks"]

    def __init__(self, INPUT=None, OUTPUT=None, LANGUAGES="en", MAX_RETRIES=10, MAX_TALKS=None):
        super().__init__()

        self.languages = set(LANGUAGES.split(','))
        if not INPUT:
            # if no input file is provided, request the transcript for the latest talks
            talks = []
            with open('all.jsonl', 'r') as f:
                for line in f:
                    line = json.loads(line)
                    if set(self.languages).issubset(line['languages']):
                        talks.append(line['name'])

            self.df = pd.DataFrame(talks, columns=['name'])
            print(f"Extracting transcripts for {len(self.df)} talks for languages: {self.languages}")
        else:
            self.df = pd.read_csv(INPUT)

        # Set the index of the dataframe to the name or id column depending on which is present
        # Some older datasets use the 'id' (1136) column as the index while newer datasets use the 'name' (aicha_el_wafi_phyl...) column
        self.name_index = "name" in self.df.columns
        self.df.set_index("name" if self.name_index else "id", inplace=True)
        self.gendered_talks = "gender" in self.df.columns

        self.max_retries = int(MAX_RETRIES)
        self.max_talks = int(MAX_TALKS) if MAX_TALKS else len(self.df)

        self.finished_talks = {}

        if OUTPUT:
            if not os.path.exists(OUTPUT): # Create the output file if it does not exist
                open(OUTPUT, 'w').close()

            with open(OUTPUT, 'r+') as f:
                for line in f:
                    data = json.loads(line)
                    if self.name_index:
                        self.finished_talks[data['TALK-NAME']] = True
                    else:
                        self.finished_talks[data['TALK-ID']] = True # To keep track of the finished talks

        self.df.drop(index=self.finished_talks.keys(), inplace=True, errors='ignore') # Remove the finished talks from the dataframe

    def start_requests(self):
        """
        For each talk in the dataframe, request the transcript in the required languages
        """
        for talk_id in self.df.index:
            if talk_id not in self.finished_talks and len(self.finished_talks) < self.max_talks:
                url = f"https://www.ted.com/talks/{talk_id}" if self.name_index  else f"https://www.ted.com/talks/view/id/{talk_id}"
                yield scrapy.Request(url=url, callback=self.check_languages, meta={'talk_id': talk_id, 'finished_talks': self.finished_talks}, dont_filter=True)

    def check_languages(self, response):
        """
        Check if the required languages are available for the talk and request the transcript if they are
        """
        talk_id = response.meta['talk_id']
        talk_name = response.url.split('/')[-1]
        languages = response.css('link[rel="alternate"][hreflang][hreflang!="x-default"]::attr(href)').getall()
        languages = [lang.split('=')[-1] for lang in languages]
        finished_talks = response.meta['finished_talks']

        if self.languages.issubset(set(languages)):
            talk = {'data': {}, 'languages': set()}  # To keep track of the languages that have been processed
            for language in self.languages:
                url = f"{response.url}/transcript?language={language}"
                yield scrapy.Request(url=url, callback=self.parse_talk, meta={'talk_id': talk_id, 'talk_name': talk_name, 'language': language, 'retries': self.max_retries, 'talk': talk, 'finished_talks': finished_talks}, dont_filter=True)
        else:
            print(f"Skipping {talk_id} as not all required languages are available.")
            finished_talks[talk_id] = True

    def parse_talk(self, response):
        """
        Parse the transcript for the required language and save it to the output file if all transcripts are available
        """

        talk_id = response.meta['talk_id']
        talk_name = response.meta['talk_name']
        expected_language = response.meta['language']
        retries = response.meta['retries']
        talk = response.meta['talk']
        finished_talks = response.meta['finished_talks']

        if not retries: # Set a limit on the number of retries to limit stress on the server
            print(f"MaxRetriesReached: TALK_ID: {talk_id} Language: {expected_language} - Max retries reached")
            return
        
        if len(finished_talks) >= self.max_talks:
            print(f"\033[92mMaxTalksReached\033[00m")
            return

        data = response.css('script#__NEXT_DATA__::text').get()
        data = json.loads(data)['props']['pageProps']['transcriptData']['translation']
        language = data['language']['internalLanguageCode'] # Get the language of the received transcript

        if language in self.languages:
            transcript = response.css('script[type="application/ld+json"]::text').get()
            transcript = html.unescape(json.loads(transcript)['transcript'])
            talk['data'][language] = transcript # Save the transcript
            talk['languages'].add(language) # Add the language to the set of languages that have been processed

            if len(talk['languages']) == len(self.languages) and talk_id not in finished_talks:
                finished_talks[talk_id] = True

                item = {'TALK-ID': talk_id, 'TALK-NAME': talk_name}
                if self.gendered_talks:
                    item['GENDER'] = self.df.loc[talk_id]['gender']
                item['TRANSCRIPTS'] = talk['data']
                print(f"\033[92mFinished: TALK_ID: {talk_id} TRANSCRIPTS: {talk['languages']}\033[00m")

                yield item

            elif expected_language not in talk['languages']: # If the expected language is not in the set of languages that have been processed, request the transcript again
                print(f"Retrying: TALK_ID: {talk_id} LANGUAGE: {expected_language}")
                yield scrapy.Request(url=response.url, callback=self.parse_talk, meta={'talk_id': talk_id, 'talk_name': talk_name, 'language': expected_language, 'retries': retries-1, 'talk': talk, 'finished_talks': finished_talks}, dont_filter=True)


        else:
            # If the language of the received transcript does not match the expected language, request the transcript in the expected language again
            print(f"LanguageMismatchError: TALK_ID: {talk_id} LANGUAGE: {language}")
            yield scrapy.Request(url=response.url, callback=self.parse_talk, meta={'talk_id': talk_id, 'talk_name': talk_name, 'language': expected_language, 'retries': retries-1, 'talk': talk, 'finished_talks': finished_talks}, dont_filter=True)





