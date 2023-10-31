# low-priority-CCG-visit-RCT

40 CCGs were randomised, and half received an invitation to have a structured feedback session delivered by a senior NHS England representative to explore their implementation of guidance on low priority prescribing.

Here is the protocol on [Figshare](https://figshare.com/articles/journal_contribution/Protocol_for_a_Randomised_controlled_trial_of_structured_Educational_sessions_to_Clinical_Commissioning_Groups_and_Assessing_the_impact_on_primary_care_Prescribing/7201079)


# Setup

Create a virtualenv:

    mkvirtualenv -p /usr/bin/python3 lpvisitccg
    workon lpvisitccg

Install requirements:

    pip install -r requirements

Install the virutalenv as a kernel for jupyter to use>:

    python -m ipykernel install --user --name lpvisitccg --display-name "Python (lpvisitccg)"

In order to run the parts that query the Google Analytics API, you
need to obtain the files `analyticsreporting.dat` and
`client_secrets.json` and place them in `notebooks/`.


# Testing

There is a simple script which ensures all the notebooks can be successfully run to completion. It does nothing to test the results are as expected. Run it like this:

    python test.py
