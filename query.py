"""
# What is currently failing and why?

- Globus links in the ORNL Solr (and therefore Globus) indices are wrong. See the
  response from this:

  https://esgf-node.ornl.gov/esg-search/search?type=File&dataset_id=CMIP6.CMIP.NCAR.CESM2.historical.r1i1p1f1.Amon.tas.gn.v20190308%7Cesgf-node.ornl.gov

  wherein you will find this Globus info:

  globus:dea29ae8-bb92-4c63-bdbc-260522c92fe8/css03_data/CMIP6/CMIP/NCAR/CESM2/historical/r1i1p1f1/Amon/tas/gn/v20190308/tas_Amon_CESM2_historical_r1i1p1f1_gn_185001-201412.nc|Globus|Globus

  This link will change in form in the new index, but even the path above is not
  correct. Browsing on the endpoint via globus.org, you will see that we need a
  `esg_dataroot` prepended:

  esg_dataroot/css03_data/CMIP6/CMIP/NCAR/CESM2/historical/r1i1p1f1/Amon/tas/gn/v20190308/

  I imagine that we didn't care about the exact path because Zach could change how
  things are mapped for the https links to implicitly include that path. However, the
  globus "links" do not work the same and I think need to have the proper path.

- Globus seems to have a limit of 10000 total items in a request (even paginated). My
  ALCF index request succeeds to launch but I notice that it is transferring exactly
  10,000 files. This seems suspicious to me--we should confirm that this is a real
  limitation. Seems so:

  https://github.com/globus/globus-sdk-python/blob/813dfc4580e2be93c51cfd072208ca8494b58543/src/globus_sdk/services/search/client.py#L263

  I think I can overcome this by manually creating a paginator object like I did to
  overcome the limit problem:

  https://github.com/esgf2-us/intake-esgf/blob/22e9aeab2467ac23ff0ed4a7be0ec31c3bb7c63e/intake_esgf/core/globus.py#L123

  where I can then change `max_total_results`. The issue is that we have to set this to
  something and there is always a chance that it isn't high enough.

- Large file info requests fail for the Solr indices because they are made with requests
  to the REST API of the form`type=File&dataset_id=blah&dataset_id=blah...` and you
  quickly run out of the maximum string you can pass. We could fix this by manually
  paging the requests on our end, but as Solr is going away I am not so inclined to do
  this. 

- The globus transfer feature of intake-esgf is undermined by the requirement of some
  endpoints that the transfer requests occur from a particular domain. For example, if I
  am trying to transfer files from the OLCF/ALCF ESGF Data Node to the OLCF DTN, I get
  the following error:

  {
    "DATA_TYPE": "not_from_allowed_domain#1.0.0", "allowed_domains":
    ["sso.ccs.ornl.gov","clients.auth.globus.org"]
  }

  If I try to transfer a file manually from inside www.globus.org, this works fine. This
  is after I go through the auth process and give permission from the task. After all
  the security obstacle course, we aren't even allowed to initiate a transfer outside of
  the web app. Maybe I am interpretting this incorrectly.

"""

import intake_esgf
import pandas as pd
from intake_esgf import ESGFCatalog

# Globus won't allow you transferring to a hidden directory and so we change the cache
intake_esgf.conf.set(
    local_cache=["/home/nate/esgf-data"],
)

# For ILAMB analysis, we want models that have a carbon cycle. We will test for this by
# the presence of the following variables.
cat = ESGFCatalog().search(
    experiment_id="historical",
    variable_id=[
        "cSoilAbove1m",
        "cSoil",
        "cVeg",
        "gpp",
        "lai",
        "nbp",
        "netAtmosLandCO2Flux",
    ],
    frequency="mon",
)


# This returns a few thousand results. Let's weed them out by only using 'complete'
# unqiue combinations of (source_id, member_id, grid_label).
def has_carbon_cycle(df):
    variables = set(df.variable_id)
    # must have one of these...
    if variables.isdisjoint(["cSoil", "cSoilAbove1m"]):
        return False
    # ...and one of these...
    if variables.isdisjoint(["nbp", "netAtmosLandCO2Flux"]):
        return False
    # ...and all of these.
    if variables.issuperset(["cVeg", "gpp", "lai"]):
        return True
    return False


# We also only want a single ensemble member, so remove all but the 'smallest'
cat.remove_incomplete(has_carbon_cycle).remove_ensembles()

# To make the searches faster, let's do a search per model group and buildup a larger
# dataframe.
groups = cat.model_groups()
dfs = []
for source_id, member_id, grid_label in groups.index:
    print(f"Searching for {source_id=} {member_id=} {grid_label=}...")
    cat = ESGFCatalog().search(
        source_id=source_id,
        member_id=member_id,
        grid_label=grid_label,
        experiment_id="historical",
        variable_id=[
            "burntFractionAll",
            "cSoilAbove1m",
            "cSoil",
            "cVeg",
            "evspsbl",
            "fBNF",
            "gpp",
            "hfls",
            "hfss",
            "lai",
            "mrro",
            "mrsos",
            "nbp",
            "netAtmosLandCO2Flux",
            "pr",
            "ra",
            "rh",
            "hurs",
            "rlds",
            "rlus",
            "rsds",
            "rsus",
            "snw",
            "tas",
            "tasmax",
            "tasmin",
            "tsl",
        ],
        frequency="mon",
    )
    dfs.append(cat.df)
cat.df = pd.concat(dfs)

#
# ds = cat.to_path_dict(
#    globus_endpoint="285fafe4-ae63-11ee-b085-4bb870e392e2", globus_path="esgf-data"
# )


"""
ALCF Data Node
/css03_data/CMIP6/CMIP/NCAR/CESM2/historical/r1i1p1f1/Amon/pr/gn/v20190401/
pr_Amon_CESM2_historical_r1i1p1f1_gn_185001-201412.nc
"""
