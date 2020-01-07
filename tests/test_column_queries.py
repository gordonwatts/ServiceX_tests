# A number of queries that test that the system works pretty well.
from tests.config import running_backend  # noqa
from tests.servicex_test_utils import wait_for_request_done, get_servicex_request_data
import requests
import pytest

# This can take a very long time - 15-30 minutes depending on the quality of your connection.
# If it is taking too long, most likely the problem is is the downloading - so look at the log
# from the rucio downloader to track progress (yes, an obvious feature request).

@pytest.mark.skip()
def test_column_query(running_backend):
    'Get electrons using column query'

    # Start the request off and get back the basic info about the request.
    response = requests.post(f'{running_backend}/transformation', json={
        "did": "mc15_13TeV:mc15_13TeV.361106.PowhegPythia8EvtGen_AZNLOCTEQ6L1_Zee.merge.DAOD_STDM3.e3601_s2576_s2132_r6630_r6264_p2363_tid05630052_00",
        "columns": "Electrons.pt(), Electrons.eta(), Electrons.phi(), Electrons.e(), Muons.pt(), Muons.eta(), Muons.phi(), Muons.e()",
        "image": "sslhep/servicex-transformer:latest",
        "result-destination": "object-store",
        "result-format": "parquet",
        "chunk-size": 7000,
        "workers": 1
    })
    print(response.json())
    assert response.status_code == 200
    request_id = response.json()["request_id"]
    assert isinstance(request_id, str)

    # Wait for the request to finish
    wait_for_request_done(running_backend, request_id)

    # Load the data back.
    pa_table = get_servicex_request_data(running_backend, request_id)

    assert len(pa_table) == 9800
