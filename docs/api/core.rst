Core
====

.. module:: laravel_cloud_queues

The names most applications need are exported from the top-level package.

.. code-block:: python

   from laravel_cloud_queues import Registry, RetryPolicy, current_job

Registry
--------

.. autoclass:: laravel_cloud_queues.Registry
   :members:
   :no-index:

Jobs
----

.. autoclass:: laravel_cloud_queues.Job
   :members:
   :special-members: __call__

.. autoclass:: laravel_cloud_queues.DispatchReceipt
   :members:

.. autoclass:: laravel_cloud_queues.jobs.DispatchOptions
   :members:

Retry Policy
------------

.. autoclass:: laravel_cloud_queues.RetryPolicy
   :members:

The Job Context
---------------

.. autoclass:: laravel_cloud_queues.JobContext
   :members:

.. autofunction:: laravel_cloud_queues.current_job

Configuration
-------------

.. autofunction:: laravel_cloud_queues.load_config
   :no-index:

.. autoclass:: laravel_cloud_queues.QueueConfig
   :no-index:
   :members:

.. data:: laravel_cloud_queues.__version__

   The installed package version.
