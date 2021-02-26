import pandas as pd
import numpy as np
import requests
import json
import os

class ScopusModified(object):

    def __init__(self, apikey=None):
        self.apikey = apikey


    def _parse_author(self, entry):
        #print(entry)
        author_id = entry['dc:identifier'].split(':')[-1]
        lastname = entry['preferred-name']['surname']
        firstname = entry['preferred-name']['given-name']
        doc_count = int(entry['document-count'])
        # affiliations
        if 'affiliation-current' in entry:
            affil = entry['affiliation-current']
            try:
                institution_name = affil['affiliation-name']
            except:
                institution_name = None
            try:
                institution_id = affil['affiliation-id']
            except:
                institution_id = None
        else:
            institution_name = None
            institution_id = None
        #city = affil.find('affiliation-city').text
        #country = affil.find('affiliation-country').text
        #affiliation = institution + ', ' + city + ', ' + country

        return pd.Series({'author_id': author_id, 'name': firstname + ' ' + lastname, 'document_count': doc_count,\
                'affiliation': institution_name, 'affiliation_id': institution_id})


    def _parse_article(self, entry):
        try:
            scopus_id = entry['dc:identifier'].split(':')[-1]
        except:
            scopus_id = None
        try:
            title = entry['dc:title']
        except:
            title = None
        try:
            publicationname = entry['prism:publicationName']
        except:
            publicationname = None
        try:
            issn = entry['prism:issn']
        except:
            issn = None
        try:
            isbn = entry['prism:isbn']
        except:
            isbn = None
        try:
            eissn = entry['prism:eIssn']
        except:
            eissn = None
        try:
            volume = entry['prism:volume']
        except:
            volume = None
        try:
            pagerange = entry['prism:pageRange']
        except:
            pagerange = None
        try:
            coverdate = entry['prism:coverDate']
        except:
            coverdate = None
        try:
            doi = entry['prism:doi']
        except:
            doi = None
        try:
            citationcount = int(entry['citedby-count'])
        except:
            citationcount = None
        try:
            affiliation = _parse_affiliation(entry['affiliation'])
        except:
            affiliation = None
        try:
            aggregationtype = entry['prism:aggregationType']
        except:
            aggregationtype = None
        try:
            sub_dc = entry['subtypeDescription']
        except:
            sub_dc = None
        try:
            author_entry = entry['author']
            author_id_list = [auth_entry['authid'] for auth_entry in author_entry]
        except:
            author_id_list = list()
        try:
            link_list = entry['link']
            full_text_link = None
            for link in link_list:
                if link['@ref'] == 'full-text':
                    full_text_link = link['@href']
        except:
            full_text_link = None

        return pd.Series({'scopus_id': scopus_id, 'title': title, 'publication_name':publicationname,\
                'issn': issn, 'isbn': isbn, 'eissn': eissn, 'volume': volume, 'page_range': pagerange,\
                'cover_date': coverdate, 'doi': doi,'citation_count': citationcount, 'affiliation': affiliation,\
                'aggregation_type': aggregationtype, 'subtype_description': sub_dc, 'authors': author_id_list,\
                'full_text': full_text_link})


    def _parse_entry(self, entry, type_):
        if type_ == 1 or type_ == 'article':
            return self._parse_article(entry)
        else:
            return self._parse_author(entry)


    def _search_scopus(self, key, query, type_, view, index=0):
        par = {'query': query, 'start': index,
               'httpAccept': 'application/json', 'view': view}

        insttoken = os.environ.get('INSTTOKEN')
        headers = {'X-ELS-Insttoken': insttoken, 'X-ELS-APIKey': key}

        if type_ == 'article' or type_ == 1:
            r = requests.get("https://api.elsevier.com/content/search/scopus", params=par, headers=headers)
        else:
            par['view'] = 'STANDARD'
            r = requests.get("https://api.elsevier.com/content/search/author", params=par, headers=headers)

        js = r.json()
        #print(r.url)
        total_count = int(js['search-results']['opensearch:totalResults'])
        entries = js['search-results']['entry']

        result_df = pd.DataFrame([self._parse_entry(entry, type_) for entry in entries])

        if index == 0:
            return(result_df, total_count)
        else:
            return(result_df)


    def search(self, query, count=100, type_=1, view='COMPLETE'):
        if type(count) is not int:
            raise ValueError("%s is not a valid input for the number of entries to return." %number)

        result_df, total_count = self._search_scopus(self.apikey, query, type_, view)

        if total_count <= count:
            count = total_count

        if count <= 25:
            # if less than 25, just one page of response is enough
            return result_df[:count]

        # if larger than, go to next few pages until enough
        i = 1
        while True:
            index = 25*i
            result_df = result_df.append(self._search_scopus(self.apikey, query, type_, view=view, index=index),
                                         ignore_index=True)
            if result_df.shape[0] >= count:
                return result_df[:count]
            i += 1


    def parse_citation(self, js_citation, year_range):
        resp = js_citation['abstract-citations-response']
        cite_info_list = resp['citeInfoMatrix']['citeInfoMatrixXML']['citationMatrix']['citeInfo']

        year_range = (year_range[0], year_range[1]+1)
        columns = ['scopus_id', 'previous_citation'] + [str(yr) for yr in range(*year_range)] + ['later_citation', 'total_citation', 'range_citation']
        citation_df = pd.DataFrame(columns=columns)

        year_arr = np.arange(year_range[0], year_range[1]+1)
        for cite_info in cite_info_list:
            cite_dict = {}
            # dc:identifier: scopus id
            cite_dict['scopus_id'] = cite_info['dc:identifier'].split(':')[-1]
            # pcc: previous citation counts
            try:
                cite_dict['previous_citation'] = cite_info['pcc']
            except:
                cite_dict['previous_citation'] = pd.np.NaN
            # cc: citation counts during year range
            try:
                cc = cite_info['cc']
            except:
                return pd.DataFrame()
            for index in range(len(cc)):
                year = str(year_arr[index])
                cite_dict[year] = cc[index]['$']
            # lcc: later citation counts
            try:
                cite_dict['later_citation'] = cite_info['lcc']
            except:
                cite_dict['later_citation'] = pd.np.NaN
            # rowTotal: total citation counts
            try:
                cite_dict['total_citation'] = cite_info['rowTotal']
            except:
                cite_dict['total_citation'] = pd.np.NaN

            try:
                cite_dict['range_citation'] = cite_info['rangeCount']
            except:
                cite_dict['range_citation'] = pd.np.NaN

            citation_df = citation_df.append(cite_dict, ignore_index=True)

        return citation_df[columns]


    def retrieve_citation(self, scopus_id_array, year_range):
        date = '%i-%i' %(year_range[0], year_range[1])

        par = {'scopus_id': ','.join(scopus_id_array), \
                'httpAccept':'application/json', 'date': date}

        insttoken = os.environ.get('INSTTOKEN')
        headers = {'X-ELS-Insttoken': insttoken, 'X-ELS-APIKey': self.apikey}

        r = requests.get("https://api.elsevier.com/content/abstract/citations", params=par, headers=headers)
        js = r.json()

        return self.parse_citation(js, year_range)

