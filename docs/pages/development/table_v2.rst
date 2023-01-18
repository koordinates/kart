Table Datasets V2
=================

:doc:`Table Datasets V3 </pages/development/table_v3>` is the current table dataset storage format
Kart uses, and is a good starting point for learning about Table Datasets V2.
Kart 0.10 continues to support Table Datasets V2, but all newly created repos
will use Table Datasets V3.

Differences
~~~~~~~~~~~

A V2 table dataset is exactly like a V3 table dataset, except:

-  The folder that contains the entire dataset is called
   ``.sno-dataset`` instead of ``.table-dataset``.
-  Attaching a particular path structure to the dataset within the
   ``path-structure.json`` meta item is not supported - instead, all V2
   datasets use the same path structure, known as the legacy path
   structure.

Legacy path-structure
^^^^^^^^^^^^^^^^^^^^^

The legacy path structure information isn't written to a
``path-structure.json`` file, but if it was, it would look as follows:

.. code:: json

   {
     "scheme": "msgpack/hash",
     "branches": 256,
     "levels": 2,
     "encoding": "hex"
   }

This means that every feature path looks something like the following:

``3c/57/kU0=``

This example is for a feature with one primary key column only, and a
primary key value of ``[77]``.

To generate the path to the file:
'''''''''''''''''''''''''''''''''

``[77]`` -> MessagePack -> ``bytes([0x91, 0x4d])`` -> SHA256 ->
``bytes([0x3c, 0x57, 0x8e, 0x75, ...])`` -> hex encode first two bytes
as a 2-level path -> ``3c/57``

To generate the filename:
'''''''''''''''''''''''''

``[77]`` -> MessagePack -> ``bytes([0x91, 0x4d])`` -> Base64 -> ``kU0=``
