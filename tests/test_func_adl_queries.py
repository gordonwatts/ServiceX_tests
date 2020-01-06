# A number of queries that test that the system works pretty well.
from tests.config import running_backend  # noqa
from tests.servicex_test_utils import wait_for_request_done, get_servicex_request_data
import requests

# This can take a very long time - 15-30 minutes depending on the quality of your connection.
# If it is taking too long, most likely the problem is is the downloading - so look at the log
# from the rucio downloader to track progress (yes, an obvious feature request).
def test_func_adl_query_simple_jets(running_backend):
    'Get jet pts using column query'

    # Start the request off and get back the basic info about the request.
    response = requests.post(f'{running_backend}/transformation', json={
        "did": "mc15_13TeV:mc15_13TeV.361106.PowhegPythia8EvtGen_AZNLOCTEQ6L1_Zee.merge.DAOD_STDM3.e3601_s2576_s2132_r6630_r6264_p2363_tid05630052_00",
        "selection": "(call ResultTTree (call Select (call SelectMany (call EventDataset (list 'localds:bogus')) (lambda (list e) (call (attr e 'Jets') 'AntiKt4EMTopoJets'))) (lambda (list j) (/ (call (attr j 'pt')) 1000.0))) (list 'JetPt') 'analysis' 'junk.root')",
        "image": "sslhep/servicex_xaod_cpp_transformer:develop",
        "result-destination": "object-store",
        "result-format": "root-file",
        "chunk-size": 1000,
        "workers": 5
    })
    assert response.status_code == 200
    request_id = response.json()["request_id"]
    assert isinstance(request_id, str)

    # Wait for the request to finish
    wait_for_request_done(running_backend, request_id)

    # Load the data back.
    pa_table = get_servicex_request_data(running_backend, request_id)

    print(pa_table)
    assert len(pa_table) == 147688

def test_func_adl_query_simple_jets_large_sample(running_backend):
    'Do not run this unless you have a large well connected system at your beck and call!!'

    # Start the request off and get back the basic info about the request.
    response = requests.post(f'{running_backend}/transformation', json={
        "did": "mc15_13TeV:mc15_13TeV.361106.PowhegPythia8EvtGen_AZNLOCTEQ6L1_Zee.merge.DAOD_STDM3.e3601_s2576_s2132_r6630_r6264_p2363_tid05630052_00",
        "selection": "(call ResultTTree (call Select (call SelectMany (call EventDataset (list 'localds:bogus')) (lambda (list e) (call (attr e 'Jets') 'AntiKt4EMTopoJets'))) (lambda (list j) (/ (call (attr j 'pt')) 1000.0))) (list 'JetPt') 'analysis' 'junk.root')",
        "image": "servicexxaodcpptransformer:latest",
        "result-destination": "object-store",
        "result-format": "root-file",
        "chunk-size": 1000,
        "workers": 100
    })
    assert response.status_code == 200
    request_id = response.json()["request_id"]
    assert isinstance(request_id, str)

    # Wait for the request to finish
    wait_for_request_done(running_backend, request_id)

    # Load the data back.
    pa_table = get_servicex_request_data(running_backend, request_id)

    print(pa_table)
    assert len(pa_table) == 147688
