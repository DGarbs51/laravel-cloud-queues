Extension Points
================

These modules are used by framework adapters and advanced integrations. Their
signatures may still change during the 0.x releases. See :doc:`/extending`.

Registry Protocols
------------------

.. autoclass:: laravel_cloud_queues.registry.WorkerTarget
   :members:

.. autoclass:: laravel_cloud_queues.registry.Invoker
   :members:

.. autoclass:: laravel_cloud_queues.registry.DefaultInvoker
   :members:

Worker
------

.. automodule:: laravel_cloud_queues.worker
   :members: Worker, WorkerOptions, resolve_target

CLI
---

.. automodule:: laravel_cloud_queues.cli
   :no-members:

.. autofunction:: laravel_cloud_queues.cli.main

.. data:: laravel_cloud_queues.cli.cli

   The ``click`` command group behind the ``laravel-cloud-queues`` console script.

Codecs
------

.. automodule:: laravel_cloud_queues.codecs
   :members:

Transports
----------

.. automodule:: laravel_cloud_queues.transports.base
   :members: Producer, Consumer, AsyncProducer, AsyncConsumer, OutgoingMessage, SentMessage, Delivery
