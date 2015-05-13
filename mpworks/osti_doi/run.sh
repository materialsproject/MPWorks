#!/bin/bash

mpworks=$HOME/MPWorks
cd $mpworks
workon env_mp_osti_doi
export PYTHONPATH=`pwd`:$PYTHONPATH
mgbuild run -v mpworks.osti_doi.builders.DoiBuilder nmats=1 dois=dois.json materials=materials.json
