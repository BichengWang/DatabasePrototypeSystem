# DatabasePrototypeSystem

## A database prototype system follow UCI CS222P https://grape.ics.uci.edu/wiki/public/wiki/cs222p-2017-fall

## Four-Level implemented with seperate test cases.

# Record-based File(RBF) folder: Record-Based File Manager (RBF) by Bicheng Wang:

  Page File(PF) component， provide facilities for higher-level control of file I/O of pages;
    
  Record-based File component, handle the record CRUD in file page system, support variable length data compressively save in page system.

# Relation Manager(RM) folder: Relation Manager (RM) by Yuhang Wu

Base on the record-based file manager level, implmented relation management between different tables.
  
Manage Catalog table: hold all information of my database, including table info, each tables' columns info, related files' name info.
  
Two main tables: Tables' table, Columns' table.

And other user tables.

# Index Manager(IX) folder: Index Manager (IX) by Bicheng Wang

implement B+ tree to support range predicates:
  
support data insert, update, delete, and bulk load in B+ tree.


# Query Engine(QE) folder: Query Engine (QE) by Yuhang Wu & Bicheng Wang

Based on Relation Manger component, Query Engine component include:
  
 Relation Manager index scan iterator.
 Filter Interface: filter the tuple by selection condition. 
 Projection Interface: project out the value of attributes.
 Block Nested-Loop Join Interface: takes two iterators, leftIn as outer, rightIn as inner relation.
 Index Nested-Loop Join Interface: base on nested loop to join.
 Grace-Hash Join Interface: support "equal" join
 Aggregate Interface: support max, min, sum, average, count.
