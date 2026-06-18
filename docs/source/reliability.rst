Reliability and Error Handling
==============================

Public exception boundary
-------------------------

Every public function in ``TM1_bedrock_py.bedrock`` has a common exception
boundary. On failure it:

* preserves the original exception type, traceback, and chained cause;
* adds the public operation path and useful identifiers;
* logs one structured outermost failure for nested Bedrock calls;
* redacts password, token, secret, credential, API-key, SQL, and MDX values;
* summarizes DataFrames and service objects instead of serializing them.

Callers can continue catching existing exception classes:

.. code-block:: python

   try:
       bedrock.load_csv_data_to_tm1_cube(...)
   except FileNotFoundError:
       ...
   except ValueError:
       ...

Parallel failure behavior
-------------------------

Async executors wait for all scheduled workers. If one or more workers fail,
the executor raises ``RuntimeError`` with the failed worker indexes and chains
the first worker failure. Rendered SQL and MDX statements are removed from
worker error output.

Transaction and cleanup behavior
--------------------------------

Built-in SQL writers and clear functions:

* validate routing and structural inputs before opening a cursor;
* commit successful operations;
* attempt rollback after database failures;
* preserve the primary database exception if rollback or close also fails;
* close cursors and library-owned connections without requiring cursor context
  manager support.

The native-view TM1 extractor tracks temporary subsets and views. It attempts
independent cleanup after success or failure. Cleanup errors do not replace an
existing extraction error; if extraction succeeded but requested cleanup
failed, the call raises a cleanup ``RuntimeError``.

Clear ordering
--------------

Clearing is destructive and opt-in.

* Target clears occur before a non-empty write.
* Async target clears occur once before worker fan-out where documented.
* Source clears occur only after a successful, non-empty export.
* Empty transformed exports skip both the target write and source clear.

Trusted executable input
------------------------

Caller-provided SQL, delete statements, MDX, and custom callables are trusted
developer configuration. Bedrock does not attempt to make arbitrary statements
safe. Never build them from untrusted user input without validation and
parameterization appropriate to the database or TM1 query layer.

Validation status
-----------------

The offline hardening suite validates wrapper orchestration with
``MockTM1Service``, temporary SQL/CSV fixtures, dimension processing, async
failure propagation, query redaction, rollback, and cleanup.

Live TM1 contract tests require a configured Planning Analytics server.
Authentication, privileges, server version, REST behavior, and production SQL
dialects must still be validated in the deployment environment.
