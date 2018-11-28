# low-priority-CCG-visit-RCT

20 CCGs will receive an invitation to have a structured feedback session delivered by a senior NHS England representative to explore their implementation of guidance on low priority prescribing.

Here is the protocol: https://docs.google.com/document/d/1rYlEG5MUgkXDvw34G8XgkWmXoelLswPVWVBWZ1o0LgU/edit


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

# Development notes and recommendations

 * Make a repo for each paper. Work towards using a standard layout (currently evolving of course)
 * Include a link to the protocol in the README
 * Use virtualenv and `requirements.txt` to define a working environment
 * Use [jupytext](https://github.com/mwouts/jupytext) to maintain a python version of the notebook along the standard format.  This allows you to:
   * Visualise changes properly in git
   * Use a text editor to check formatting
 * Use PEP8 as a style guide
 * Every time you query an external data source (e.g. BigQuery), be sure to save a copy as a CSV in the repository (preferably in a `data/` subdirectory). This way people can check your analyses even if they don't have the source data
 * For expensive queries, you can optionally wrap them with a `DUMMY_RUN` flag which will load a previously-saved CSV, if it exists
 * Never rely on "black box" data sources, such as intermediate tables created by other people or yourself in the past. It should be possible to recreate your analysis from the most basic raw data sources: (1) so others can check your work (2) so you have a repeatable scenario to check and work with
 * If your analysis relies on the state of something else (e.g. the measures as defined in OpenPrescribing on a certain date), find a way to incorporate that state into your code if possible - for example using a git commit hash
 * Put complex logic in functions, in separate, importable files. The main notebook should be limited to standard data reshaping operations (merging, pivots, matrix algebra etc). Consider wrapping these functions up into a packaged module over time
 * Keep these guidelines up to date as we evolve them
 * Check all your notebooks can be run from start to finish without error, preferably as part of a test script (see above)
