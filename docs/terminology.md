# Terminology

<table>
    <tr>
      <th>Term
      <th>Definition
    </tr>
    <tr>
      <td>Base Attribute
      <td>A data attribute received from a source system, e.g. DOB.
    </tr>
    <tr>
      <td>Transformed Attribute
      <td>A data attribute created as a result of a transformation, e.g. Age from DOB. The code to implement a transformation could be SQL, Python, R, Scala, etc.
    </tr>
    <tr>
      <td>Derived Attribute
      <td>An output variable of an analytical model. Output variables could be used as input features to another model.
    </tr>
    <tr>
      <td>EAVT
      <td>Stands for Entity - Attribute - Value - Time. A table structure whereby each value for a given entity at a given time is a row. An entity may be Customer, Product, Household, etc.
    </tr>
    <tr>
      <td>Feature Vector
      <td>A data structure whereby each feature is represented as a column, with one row per entity, e.g. one row per customer.
    </tr>
    <tr>
      <td>Wide Table
      <td colspan="1">An alias for Feature Vector.
    </tr>
    <tr>
      <td>Data Vault
      <td>
        <p>Data Vault is designed to enable parallel loading, and is designed to be resilient to change in the business environment by explicitly separating structural information from descriptive attributes. It makes no distinction between good and bad data ("bad" meaning not conforming to business rules), since data data may have predictive value.
      </td>
    </tr>
</table>