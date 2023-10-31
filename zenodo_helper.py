import requests
import json
import os

zenodo_baseurl = "https://zenodo.org/api/"
headers = {"Content-Type": "application/json"}

# SEASOI TOKEN ZENODO official
ACCESS_TOKEN = open('zenodo_access_token.txt','r').read()


# params = {'access_token': ACCESS_TOKEN}

def zenlist_all(base_url, token):
    r = requests.get(base_url + 'deposit/depositions',
                     params={'access_token': token})
    return r


def zenlist_all_query(base_url, token, query):
    r = requests.get(base_url + 'deposit/depositions',
                     params={'q': query,
                             'access_token': token})
    return r


def zenlist_single(base_url, token, record_id):
    r = requests.get(base_url + "deposit/depositions/" + record_id,
                     params={'access_token': token})
    return r


def zenlist_single_files(base_url, token, record_id):
    r = requests.get(base_url + "deposit/depositions/" + record_id + "/files",
                     params={'access_token': token})
    return r


def zen_newversion(base_url, token, record_id):
    r = requests.post(base_url + "deposit/depositions/" + record_id + "/actions/newversion",
                      params={'access_token': token})
    return r


def zen_unlock_submited(base_url, token, record_id):
    r = requests.post(base_url + 'deposit/depositions/' + record_id + '/actions/edit',
                      params={'access_token': token})
    return r


def check_token(base_url, token):
    """
    Function to check if Zenodo token is allowed to deposit files
    """
    r = requests.post(base_url + "deposit/depositions",
                      params={'access_token': token},
                      json={},
                      headers=headers)
    if r.status_code == 201:
        print("Allowed to deposit some files")
    else:
        print("Please check your token or url")
    return r


def zenvar(requests_response):
    """
    Store 3 variables in a list:
    - bucket_url
    - Reserved DOI
    - Record id
    """
    bucket_url = requests_response.json()["links"]["bucket"]
    reserved_doi = requests_response.json()["metadata"]["prereserve_doi"]["doi"]
    recid = requests_response.json()["metadata"]["prereserve_doi"]["recid"]
    return [bucket_url, reserved_doi, recid]


def zenul(bucket_url, token, folder, filename):
    """
    Upload files to Zenodo
    Use the New API
    The target URL is a combination of the bucket link with the desired filename seperated by a slash.
    Usage:
    requests_response: answered from check_token
    bucket_url: answer from zenvar
    folder: folder where your archive is available
    filename: filename to upload
    """
    for i in range(len(filename)):
        print("upload: " + filename[i])
        # absfilename = os.path.join(folder, filename[i])
        with open(filename[i], "rb") as fp:
            r = requests.put(
                "%s/%s" % (bucket_url, filename[i].split('/')[-1]),
                data=fp,
                params={'access_token': token},
            )
    return r



def zenul2(base_url, record_id, token, folder, filename):
    """
    Upload new files to Zenodo
    The target URL is a combination of the bucket link with the desired filename seperated by a slash.
    Usage:
    requests_response: answered from check_token
    bucket_url: answer from zenvar
    folder: folder where your archive is available
    filename: filename to upload
    """
    for i in range(len(filename)):
        print("upload: " + filename[i])
        absfilename = os.path.join(folder, filename[i])
        with open(absfilename, "rb") as fp:
            r = requests.post(base_url + "deposit/depositions/" + record_id + "/files",
                              data={'name': fp},
                              files={'file': open(absfilename, 'rb')},
                              params={'access_token': token},
                              )

    return r


def creator_dict(df, i):
    """
    Format Creator field to dict
    """
    creator = df.iloc[i]["Creator"].split("_\n")[0].split(":")[1].split(",")
    for cdi in range(len(creator)):
        cdict = {}
        cdict["name"] = creator[cdi]
        creator[cdi] = cdict
    return creator


def zenmdt(base_url, token, record_id, df, i):
    """
    Set Metadata from Dataframe
    """
    data = {
        #  "title": df.iloc[i]["Title"].split(':')[1],
        'metadata': {
            'title': df.iloc[i]["Title"].split(':')[1],
            "publication_date": df.iloc[i]['Date'].split("_\n")[0].split(':')[1],
            'description': df.iloc[i]["Description"].split("_\n")[0].split(':')[1].replace('\n', '<br />') + "<br />" +
                           df.iloc[i]["Provenance"].split("_\n")[0].replace('\n', '<br />'),
            "access_right": "open",
            'creators': creator_dict(df, i),
            "keywords": df.iloc[i]["Subject"].split("_\n")[0].split(":")[1].split(","),
            "related_identifiers": [{
                "identifier": "urn:{}".format(df.iloc[i]["Identifier"].split('_\n')[0].split(':')[1]),
                "relation": "isIdenticalTo",
                "scheme": "urn"
            }],
            "version": str(df.iloc[i]["Description"].split("edition:")[1].split("_\n")[0]),
            "language": df.iloc[i]['Language'],
            "license": "cc-by-4.0",
            "imprint_publisher": "Zenodo",
            "upload_type": df.iloc[i]['Type'],
            # "communities":[{'identifier':'uav'},
            #                {'identifier':'ecfunded'}],
            # "license": {
            #         "id": "CC-BY-4.0"
            #             },
            # "grants": [{"funder": {
            #   "doi": "10.13039/501100000780",
            #   "acronyms": [
            #     "EC"
            #   ],
            #   "name": "European Commission",
            #   "links": {
            #     "self": "https://zenodo.org/api/funders/10.13039/501100000780"
            #   }
            # }
            #              }]
        }
    }
    print(data)
    r = requests.put(base_url + "deposit/depositions/" + str(record_id),
                     params={'access_token': token},
                     data=json.dumps(data),
                     headers=headers)

    return r


def zenpublish(base_url, token, record_id):
    "Publish !"
    r = requests.post(base_url + "deposit/depositions/" + str(record_id) + "/actions/publish",
                      params={'access_token': token})
    return r


### JSON config for Zenodo
def zenodo_json(fname, type, output, out_prefix):
    """
    JSON configuration for Zenodo with optionnal type parameter
    if type is not defined, regular zenodo will be use.
    if type='sandbox': Zenodo Sandbox will be use !
    """
    if type == 'sandbox':
        zenodo_url = "https://sandbox.zenodo.org/api"
    else:
        zenodo_url = "https://zenodo.org/api"

    zen_json = {'profile': {'id': 'rawdata', 'name': 'rawdata', 'project': 'Publish rawdata with Geoflow',
                            'organization': 'IRD', 'environment': {'file': '.env', 'hide_env_vars': ['MOTDEPASSE']},
                            'logos': ['https://en.ird.fr/sites/ird_fr/files/2019-08/logo_IRD_2016_BLOC_UK_COUL.png'],
                            'mode': 'entity'}, 'metadata': {
        'entities': [{'handler': 'csv', 'source': os.path.join(output, out_prefix) + "_zenodo-rawdata.csv"}],
        'contacts': [
            {'handler': 'csv', 'source': 'https://drive.ird.fr/s/EYS3qccyB28PrA9/download/geoflow_g2oi_contacts.csv'}]},
                'software': [{"id": "my-zenodo", "type": "output", "software_type": "zenodo",
                              "parameters": {"url": zenodo_url, "token": "{{ ZENODO_SANDBOX_TOKEN }}",
                                             "logger": "DEBUG"}, "properties": {"clean": {"run": False}}}], 'actions': [
            {"id": "zen4R-deposit-record",
             "options": {"update_files": True, "communities": "uav", "depositWithFiles": True, "publish": False,
                         "update_metadata": True, "strategy": "newversion", "deleteOldFiles": True}, "run": True}]}
    return zen_json

##### JSON
### Set empty dictionary
# zen_json = {}
### Parse actual json
# with open("/home/sylvain/Documents/IRD/geoflow/geoflow-g2oi/Zenodo.json", "r") as read_file:
#     print("Converting JSON encoded data into Python dictionary")
#     zen_json = json.load(read_file)