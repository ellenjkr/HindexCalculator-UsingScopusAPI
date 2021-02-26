from PyscopusModified import ScopusModified
import requests
import json
import pandas as pd


class Researcher(): # Researcher class that holds its data
	def __init__(self, name, researcher_id, documents = None, cit = None, hindex = None, documents5 = None, cit5 = None, hindex5 = None, search = None, quartis = None, period = None):
		super(Researcher, self).__init__()
		self.name = name
		self.id = researcher_id
		self.documents = documents
		self.cit = cit
		self.hindex = hindex
		self.documents5 = documents5
		self.cit5 = cit5
		self.hindex5 = hindex5

		self.search = search
		self.quartis = quartis
		self.period = period


class Data():
	def __init__(self, file_name):
		super(Data, self).__init__()
		self.file_name = file_name
		self.parse_data()
		

	def parse_data(self):
		data = pd.read_csv(self.file_name, sep=';', encoding='iso-8859-1') # Reads the csv file
		data = data.rename(columns={'HS-5':'H5', 'WS-5':'D5'}) # Changes some column names

		self.researchers = [] # A list of researchers

		for pos, researcher_id in enumerate(data["SCOPUS ID"]): # Goes through the data and gets the id's from the researchers
			name = data['Nome'][pos] # Gets the name
			if str(name) == 'nan':
				name = '-'

			if str(researcher_id) == 'nan' or str(researcher_id) == 'NaN': # If the id is null
				researcher_id = '-'
			
			if name != '-' and researcher_id !='-':
				period = {}
				if str(data["17"][pos]) != "nan" and str(data["17"][pos]) != "NaN":
					period["17"] = True
				else:
					period["17"] = False
				if str(data["18"][pos]) != "nan" and str(data["18"][pos]) != "NaN":
					period["18"] = True
				else:
					period["18"] = False
				if str(data["19"][pos]) != "nan" and str(data["19"][pos]) != "NaN":
					period["19"] = True
				else:
					period["19"] = False
				if str(data["20"][pos]) != "nan" and str(data["20"][pos]) != "NaN":
					period["20"] = True
				else:
					period["20"] = False
	
				self.researchers.append(Researcher(name = name, researcher_id = int(researcher_id), period=period)) # Adds a new researcher with its name and id
	
	def add_data(self, researchers_updated): # Adds new data (documents, citations and hindex) to each researcher. The parameter is a list of researchers
		for researcher_updated in researchers_updated:
			for researcher in self.researchers:
				if researcher.id == researcher_updated.id and researcher.id != "-": # The compatible researcher
					researcher.documents = int(researcher_updated.documents)
					researcher.cit = int(researcher_updated.cit)
					researcher.hindex = int(researcher_updated.hindex)
					researcher.documents5 = int(researcher_updated.documents5)
					researcher.cit5 = int(researcher_updated.cit5)
					researcher.hindex5 = int(researcher_updated.hindex5)
					researcher.search = researcher_updated.search
					researcher.quartis = researcher_updated.quartis

	def save_quartis(self):
		for researcher in self.researchers:
			researcher.quartis.to_csv(f"../Quartis/Autores/{researcher.name}.csv", sep=';', index=False, encoding='utf-8')


class ScopusRetriever():
	def __init__(self, data, key): # Parameters are the data and the scopus api key
		super(ScopusRetriever, self).__init__()
		self.data = data
		self.key = key
		self.scopusModified = ScopusModified(self.key) # Instance of a modified Scopus class

		self.researchers = [] # List of researchers

	def retrieve_data(self):
		for researcher in self.data.researchers: 
			if researcher.id != '-':
				search = self.scopusModified.search(f"AU-ID ({researcher.id})") # Searches for an author by its ID
				citations = self.retrieve_cit(search) # Gets the citations data

				documents = len(self.scopusModified.search(f"AU-ID ({researcher.id}) and PUBYEAR  > 1990"))

				# AU-ID ({researcher.id}) and PUBYEAR  >  2014 and PUBYEAR  <  2020
				documents_results = self.scopusModified.search(f"AU-ID ({researcher.id}) and PUBYEAR  > 2016") # Gets the documents data
				documents5 = len(documents_results) # Amount of documents published


				citations_career = self.retrieve_cit(search, True)
				cit_career_results = pd.Series(citations_career.range_citation.astype(int))
				cit = cit_career_results.sum()

				cit_results = pd.Series(citations.range_citation.astype(int)) # Calculates the amount of citations
				cit5 = cit_results.sum()

				hindex = self.calculate_hindex(citations_career)

				hindex5 = self.calculate_hindex(citations) # Calculates the hindex

				quartis = self.get_quartis_documents(search, researcher) # Get the documents info that its needed to calculate the quartis
				self.researchers.append(Researcher(researcher.name, researcher.id, documents, cit, hindex, documents5, cit5, hindex5, search, quartis)) # Add a new researcher to the list of researchers

		return self.researchers


	def retrieve_cit(self, search, career=False): # Retrieves the citations data from scopus API
		docs_array = []
		for doc in search['scopus_id']: # Gets the documents
			docs_array.append(doc)
		
		# ============== TO RETRIEVE MORE DATA THAN THE LIMIT ====================
		# The limit is 25 by request

		done = 0
		not_done = 25
		citations_temp = []
		while not_done < len(docs_array) + 25: 
			if career:
				citations_temp.append(self.scopusModified.retrieve_citation(scopus_id_array=docs_array[done:not_done], year_range=[1990, 2020]))
			else:
				citations_temp.append(self.scopusModified.retrieve_citation(scopus_id_array=docs_array[done:not_done], year_range=[2017, 2020])) # Retrieve the citations data
			done += 25
			not_done += 25
		# ========================================================================

		citations = citations_temp[0]
		for pos, citation in enumerate(citations_temp):
			if pos != 0:
				citations = citations.append(citation, ignore_index = True)

		return citations

	def get_quartis_documents(self, search, researcher):
		info = search.to_dict()
		data = {'year':[], 'title':[],'publication_name':[], 'issn':[]}
		for pos, title in enumerate(info["title"]):
			date = info["cover_date"][pos].split('-')
			date = date[0]

			if date =="2020" or date=="2019" or date=="2018" or date=="2017":
				if date == "2017" and researcher.period["17"] == False:
					continue
				elif date == "2018" and researcher.period["18"] == False:
					continue
				elif date == "2019" and researcher.period["19"] == False:
					continue
				elif date == "2020" and researcher.period["20"] == False:
					continue
				else:
					if info["aggregation_type"][pos] == "Journal" or info["aggregation_type"][pos] == "Conference Proceeding":
						data['title'].append(info["title"][pos])
						data['publication_name'].append(info["publication_name"][pos])
						if info["issn"][pos] == None:
							data['issn'].append('-')
						else:
							data['issn'].append(info["issn"][pos])
						data['year'].append(date)

		return pd.DataFrame(data)

	def calculate_hindex(self, citations): # Calculates the hindex
		citations = citations.astype({"range_citation": int})
		citations = citations.sort_values(by='range_citation', ascending=False)
		citations = citations.reset_index(drop=True)

		hindex = 0
		for pos, i in enumerate(citations['range_citation']):
			if int(i) >= (pos+1):
				hindex += 1

		return hindex


def save_data(researchers, file_name): # Parameters are the list of researchers and the new file name
	new_data_dict = {'Nome':[], 'SCOPUS ID':[], 'D':[], 'C':[], 'H':[], 'D5':[], 'C5':[], 'H5':[]}
	for researcher in researchers: # Fills the data dictionary
		new_data_dict['Nome'].append(researcher.name)
		new_data_dict['SCOPUS ID'].append(researcher.id)
		new_data_dict['D'].append(researcher.documents)
		new_data_dict['C'].append(researcher.cit)
		new_data_dict['H'].append(researcher.hindex)
		new_data_dict['D5'].append(researcher.documents5)
		new_data_dict['C5'].append(researcher.cit5)
		new_data_dict['H5'].append(researcher.hindex5)

	new_data = pd.DataFrame(new_data_dict) # Converts the dictionary to a pandas DataFrame
	new_data["SCOPUS ID"] = pd.Series(new_data["SCOPUS ID"], dtype=object)
	new_data["D"] = pd.Series(new_data["D"], dtype=object)
	new_data["C"] = pd.Series(new_data["C"], dtype=object)
	new_data["H"] = pd.Series(new_data["H"], dtype=object)
	new_data["D5"] = pd.Series(new_data["D5"], dtype=object)
	new_data["C5"] = pd.Series(new_data["C5"], dtype=object)
	new_data["H5"] = pd.Series(new_data["H5"], dtype=object)
	new_data.to_csv(file_name, sep=';', index=False, encoding='utf-8') # Converts the DataFrame to a csv file


if __name__ == '__main__':
	data = Data('atualizar.csv') # Reads the data from the file
	scopus_retriever = ScopusRetriever(data, '2f8a856ea2c32c265b4c5a9895e6900d') # Responsible for retrieving information from scopus using the data
	data.add_data(scopus_retriever.retrieve_data()) # Add the new info retrieved from scopus to the previous data
	data.save_quartis()
	save_data(data.researchers, "atualizado.csv") # Saves the new data into a file

	


