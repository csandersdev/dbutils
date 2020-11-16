select cast('Hello, World!' as CLOB) "LOB_TEXT"
     , 'hello_world.txt' "LOB_FILENAME"
  from SYS.SYSTABLES where TABLENAME = 'SYSTABLES';