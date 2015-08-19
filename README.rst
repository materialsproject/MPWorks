=======
MPWorks
=======

MPWorks merges pymatgen, custodian, and FireWorks into a custom workflow for Materials Project. It is very powerful in that it is used for all the calculations performed for the Materials Project database; however, it is also quite complicated and not completely flexible. This guide will try to explain the operation of the MPWorks system of running calculations

0. Installation
===============

MPWorks is in essence a system of running calculations. Thus, in addition to the code you need to have several MongoDB databases and environment variables set. The easiest way to install all of this is to use (or modify) `the MPenv code <https://github.com/materialsproject/MPenv>`_, which will install all the necessary dependencies of MPWorks, build the appropriate databases, and set the environment variables. Unfortunately, MPenv only works on a few systems such as NERSC and (soon) ALCF.

There are also some example workflows in MPWorks that can be attempted with much less complication. This guide will also cover those, but you will still need to learn the fundamentals and will still need a FireWorks database. It is important to note that MPWorks is not intended to be a "general purpose" code at this time, and is mainly used internally by the Materials Project team.

1. Introduction and Pre-requisites
==================================

This document is a guide for designing and running materials science and chemistry workflows using the Materials Project codebases (pymatgen, FireWorks, custodian, etc.) and NERSC resources.

The advantage of learning the infrastructure is that once you gain familiarity, you will be able to very easily run and manage your calculations. If you need to perform a set of computations over a new compound, you will simply need to execute a command rather than editing input files, ssh’ing them to NERSC, running qsub, etc…This is crucial when running hundreds of thousands of jobs, but you’ll probably find it very nice to have in your day-to-day work as well. Whenever you want to compute a new structure, you can do it in almost no time.

In addition, the infrastructure is meant to help you rigorously test your workflows over test sets of compounds and rapidly analyze the results.

However, before taking advantage of this infrastructure you must take a little time to learn it. In particular, there are many components to making the Materials Project high-throughput project function and you will need to learn at least a bit about all of them:

* The **pymatgen** codebase (http://pymatgen.org) is used to read and write input and output files for various computational codes (like VASP or NWChem) given arbitrary structures and for different computational types (like static or structure optimization). The **pymatgen-db** codebase (http://pythonhosted.org/pymatgen-db/) is used to parse output files.
* The **custodian** codebase (http://pythonhosted.org/custodian/) is used to execute the desired code in a way that can fix most errors that might be encountered during the run
* The **FireWorks** codebase (http://pythonhosted.org/FireWorks/) allows us to automically run and track many thousands of jobs (such as custodian jobs) over supercomputing resources. FireWorks can also fix more complicated errors that may arise (like server crashes) and help design dynamic workflows.

This documentation focuses on how these codebases work together and is *not* intended to teach you how to use the codebases individually. Before starting, it is therefore crucial that you review and have a basic understanding of each codebase in isolation, so things will make sense when we start putting things together.

1.1 FireWorks prerequisites
---------------------------

.. pull-quote:: | FireWorks documentation can be found at http://pythonhosted.org/FireWorks/

Before starting this documentation, make sure you have read through all the documentation on FireWorks and have a basic understanding of at least the following tutorials:

* Quickstart
* Defining Jobs using FireTasks
* Creating Workflows
* Dynamic Workflows
* Tips for designing FireTasks, FireWorks, and Workflows

This documentation assumes that you have at least a basic grasp of the concepts of those tutorials. If you are interested in not only designing and submitting workflows, but running testing or production jobs at NERSC, you should also review the following FireWorks documentation (you should read the tutorials, but you don’t have to actually follow the instructions to install anything at NERSC; remember, MPEnv will do that for you):

* Worker Tutorial
* Launch Rockets through a queue
* Reserving FireWorks upon queue submission
* Installation Notes on various clusters / supercomputing centers

1.2 custodian prerequisites
---------------------------

.. pull-quote:: | custodian documentation can be found at http://pythonhosted.org/custodian/

Additionally, you should read all the documentation on custodian (it is a single page)

1.3	pymatgen and pymatgen-db prerequisites

.. pull-quote:: | pymatgen documentation can be found at http://pythonhosted.org/pymatgen/
.. pull-quote:: | pymatgen db documentation is at http://pythonhosted.org/pymatgen-db/

Before starting this documentation, you should have enough familiarity with pymatgen to:

* write input files for the code you want to execute (e.g., VASP or NWChem) and
* parse output files for the code you want to execute into a MongoDB (JSON) format

For the latter functionality, you might need to consult the pymatgen-db codebase.








Part 1 - The basics
-------------------

There are 4(!) main databases that interact within MPenv. You have credentials for these 4 databases in the MPenv files sent to you by the MPenv admin. As a first step, you might set up a connection to these database via MongoHub (or similar) so you can easily check the contents of these databases. If you do not have a Mac, you cannot use Mongohub to check database contents, but you can either (i) skip monitoring databases directly and just use the tools built into FireWorks and other packages or (ii) use another program or just the MongoDB command line tools. You can read "The Little MongoDB book" (available for free online) to see how to use the MongoDB command line as one alternative. Mongohub is **not** by any means a requirement.

1. The most important database is the **FireWorks** database. This contains all the workflows that you want to run.

2. The 2nd most important database is the **VASP** database. This contains the results of your calculations

3. There is also a **submissions** database where you can submit Structure objects (actually SNL objects) for computation. Using this database is optional but (as demonstrated later) can be simpler than trying to create FireWorks directly.

4. Finally, there is an **SNL** database that contains all the structures you've submitted and relaxed. It is used for duplicate checking as well as record-keeping. Generally speaking, you do not need to do worry that this database exists.

One type of MPenv procedure is to submit Structures to the **submissions** database, then use an *automated* command to convert those submissions into **FireWorks** workflows and run them. The results are checked via the **VASP** database. The order of operations is  **submissions** -> **FireWorks** --> **VASP**, but your interaction is only with **submissions** and **VASP** databases.

Another type of MPenv procedure is to dispense with submissions database and instead submit workflows directly to the **FireWorks** database. In this case, your interaction is with **FireWorks** and **VASP** databases.

Part 2 - Running test workflows
-------------------------------

You can run test workflows by the following procedure. This test follows the **submissions** -> **FireWorks** --> **VASP** paradigm.

1. Log into a NERSC machine

2. Activate your environment::

    use_<ENV_NAME>

3. Note: the following command clears all your databases. Type the command::

    go_testing --clear

4. The command above clears all your databases AND submits ~40 test compounds to your **submissions** database. If you want, you can at this point try connecting to your **submissions** database (e.g. via MongoHub) and confirm that you see compounds there.

5. Items in the **submissions** database cannot be run directly. They must first be converted into FireWorks that state the actual calculations we want to perform. Type the command::

    go_submissions

6. You will see output saying that you have new workflows. This command *automatically* turned the new submissions into workflows in the **FireWorks** database that can can be run at NERSC. If you want, you can at this point try connecting to your **FireWorks** database (e.g. via MongoHub) and confirm that you see Workflows there. Or you can type ``lpad get_wflows -d less`` as another option to see what's in the FireWorks database.

7. Let's run our FireWorks by navigating to a scratch directory and using the ``qlaunch`` command of FireWorks::

    cd $GSCRATCH2
    mkdir first_tests
    cd first_tests
    qlaunch -r rapidfire --nlaunches infinite -m 50 --sleep 100 -b 10000

8. This should have submitted some jobs to the queues at NERSC. You should keep the qlaunch command running (or run it periodically) so that as workflow steps complete, new jobs can be submitted.

9. You can check progress of your workflows using the built-in FireWorks monitoring tools. Several such tools, including a web gui, are documented in the FW docs. If you want to be efficient, you will actually look this up (as well as how to rerun jobs, detect failures, etc.). Here is a simple command you can use for basic checking::

    lpad get_wflows -d more

10. When your workflows complete, you should see the results in the **VASP** database (e.g. connect via MongoHub or via pymatgen-db frontend).

Part 3 - Running custom structures
----------------------------------

You can run custom structures through the typical MP workflow very easily. You need to submit your Structures (as StructureNL objects) to your **submissions** database. Then simply use the same procedure as last time to convert those into FireWorks and run them (we are still following the **submissions** -> **FireWorks** --> **VASP** paradigm).

1. If you want, you can clear all your databases via::

    go_testing --clear -n 'no_submissions'

2. Here is some code you can use to submit a custom Structure to the **submissions** database (you will need to copy your ``<ENV_NAME>/configs/db/submission_db.yaml`` file to the location you run this code, and also have set up your MPRester API key if you want to grab a structure from Materials Project as in this example)::

    from mpworks.submission.submission_mongo import SubmissionMongoAdapter
    from pymatgen import MPRester
    from pymatgen.matproj.snl import StructureNL

    submissions_file = 'submission_db.yaml'
    sma = SubmissionMongoAdapter.from_file(submissions_file)

    # get a Structure object
    mpr = MPRester()
    s = mpr.get_structure_by_material_id("mp-149")  # this is Silicon

    # At this point, you could modify the structure if you want.

    # create an SNL object and submit to your submissions database
    snl = StructureNL(s, 'John Doe <my_email@gmail.com>')
    sma.submit_snl(snl, 'my_email@gmail.com', parameters=None)

3. Once all your structures are submitted, follow steps 5-10 in the previous part to run it.

4. There are many advanced options for setting priority, basic WF tailoring, auto-setting the submission database based on environment, etc. Consult the email list if you need help with a specific problem.

Part 4 - Running custom workflows
---------------------------------

Part 3 was about running custom *structures* through a typical MP workflow. If you want to run custom workflows (new types of calculations not coded in MP), you have a couple of options. You can either learn a bit more about MPWorks and try to code your workflow so that it can be run as in Part 3, but submitted with certain parameters (e.g., ``sma.submit_snl(snl, 'my_email@gmail.com', parameters={"calculation_type":"CUSTOM_STUFF"})``). This requires modifying the code that turns StructureNL into Workflows. In this case you are still following the **submissions** -> **FireWorks** --> **VASP** paradigm. Some (long and a bit outdated) documentation on this is in the MPWorks code in the docs folder.

The alternate strategy is to create Workflow objects directly and put them in the **FireWorks** database, bypassing the submissions database entirely. Then you are just doing  **FireWorks** --> **VASP**. Once the Workflow objects are in the **FireWorks** database, you can run them by following steps 7-10 in Part 2 of this guide (i.e., basically you just need to run the ``qlaunch`` command.

One code in development to create basic workflows that can run VASP is the **fireworks-vasp** repository (https://github.com/materialsvirtuallab/fireworks-vasp). This code can create Workflow objects that you can directly enter into your FireWorks database (the credentials for your FW database is in the ``my_launchpad.yaml`` given to you by the MPenv admin). This is not the code used by Materials Project for running workflows (MPWorks does that), but is considerably simpler to understand and modify for your needs. You can probably get started with custom workflows much more quickly with this strategy.
