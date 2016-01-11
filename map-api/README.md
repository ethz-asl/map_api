# dmap

If you use dmap in your academic work, please cite:
```tex
@inproceedings{cieslewski2015mapapi,
  title={Map API - Scalable Decentralized Map Building for Robots},
  author={Cieslewski, Titus and Lynen, Simon and Dymczyk, Marcin and Magnenat, St\'{e}phane and Siegwart, Roland},
  booktitle={Robotics and Automation (ICRA), 2015 IEEE International Conference on},
  year={2015}
}
```

## How to use dmap

### dmap data structure

Essentially, dmap provides a collection of distributed containers called 
*NetTables*, where each NetTable contains several id-referenced serialized items
of a user-defined type. These items can be shared with remote processes in a
decentralized version control scheme.

The hierarchical data structure is as follows, where each object contains
multiple objects of the class below:

```bash
NetTableManager  # Singleton. Manages access to and creation of NetTables.
  NetTable       # Explained above. A NetTable can only store items of one type.
    Chunk        # A collection of items that are shared together.
      Item       # An item representation that contains its history.
        Revision # A version of the item within its history.
```

To undestand why chunks exist as an intermediate container between NetTables and
items, we refer to the dmap paper.

### Defining custom tables

To define dmap NetTables for your types, you need to specify how your type can
be serialized into a dmap Revision. While there are many ways to do this, we
recommend to simply specify a Google Protocol Buffer serialization. 
