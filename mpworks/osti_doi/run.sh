source $HOME/.credentials
source $HOME/.virtualenvs/env_mp_osti_doi/bin/activate
cd $HOME/MPWorks
export PYTHONPATH=`pwd`:$PYTHONPATH
mgbuild run -v mpworks.osti_doi.builders.DoiBuilder nmats=20 dois=dois.json materials=materials.json
git add mpworks/osti_doi/dois.json
git commit -m "osti_doi: new dois backup"
git push origin osti_doi
