/*
 * This is a class conversion from a struct based approach, C++ to C#
 * Includes a BinaryReader to read class data from a file
 */

/*

	Symbol Data Type Description 

	B	Binary	a string 10 digits representing a .DBT block number.
				The number is stored as a string, right justified and padded with blanks. 

	C	Char	All OEM code page characters - padded with blanks to the width
					of the field. 

	D	Date	8 bytes - date stored as a string in the format YYYYMMDD.

	N	Numeric	Number stored as a string, right justified, and padded with blanks
				to the width of the field.  

	L	Logical	1 byte - initialized to 0x20 (space) otherwise T or F.

	M	Memo	a string 10 digits (bytes) representing a .DBT block number.
				The number is stored as a string, right justified and padded with blanks. 

	@	Timestamp	8 bytes - two longs, first for date, second for time.
					The date is the number of days since  01/01/4713 BC.
					Time is hours * 3600000L + minutes * 60000L + Seconds * 1000L

	I	Long	4 bytes. Leftmost bit used to indicate sign, 0 negative.

	+	Autoincrement	Same as a Long 
	F	Float Number	stored as a string, right justified, and padded with
						blanks to the width of the field.
						
	O	Double	8 bytes - no conversions, stored as a double.

	G	OLE		10 digits (bytes) representing a .DBT block number.
				The number is stored as a string, right justified and padded with blanks. 

Binary, Memo, OLE Fields and .DBT Files
Binary, memo, and OLE fields store data in .DBT files consisting of blocks
numbered sequentially (0, 1, 2, etc.). SET BLOCKSIZE determines the size of each block.
The first block in the .DBT file, block 0, is the .DBT file header. 

Each binary, memo, or OLE field of each record in the .DBF file contains the number
of the block (in OEM code page values) where the field's data actually begins.
If a field contains no data, the .DBF file contains blanks (0x20) rather than a number. 

When data is changed in a field, the block numbers may also change and the number
in the .DBF may be changed to reflect the new location.

  See:
  http://www.dbase.com/KnowledgeBase/int/db7_file_fmt.htm

  */

// Table file header

using System;
using System.IO;

class DBASEHEADER
{
    public byte m_version;                 //  1 dBASE version number
                                    // Bits 0-2 indicate version number, 3=dBASE Level 5, 4=dBASE Level 7
                                    // Bit 3 and bit 7 indicate presence of a dBASE IV or dBASE for Windows memo file
                                    // Bits 4-6 indicate the presence of a dBASE IV SQL table
                                    // Bit 7 indicates the presence of any .DBT memo file (either a dBASE III PLUS
                                    //  type or a dBASE IV or dBASE for Windows memo file).
    public byte m_lcdyear;                 //  1 Year of last update
                                           //    Add YY to 1900 (possible values 1900-2155)
    public byte m_lcdmonth;                //  1 Month of last update
    public byte m_lcdday;                  //  1 Day of last update
    public System.UInt32 m_numrecords;         //  4 Number of records in the table
                                               //    least significant byte first
    public System.UInt16 m_headerlength;        //  2 Number of bytes in the header
                                                //    least significant byte first
    public System.UInt16 m_recordlength;        //  2 Number of bytes in the record
                                                //    least significant byte first
    public System.UInt16 m_resl;                //  2 Reserved, filled with zeros
    public byte m_incompletetransaction;   //  1 Flag indicating incomplete dBASE IV transaction
    public byte m_encryptionflag;      //  1 dBASE IV encryption flag
    public System.UInt32 m_freerecordthread;   //  4 Reserved for multi-user processing
    public System.UInt32 m_res2;               //  4 Reserved for multi-user processing
    public System.UInt32 m_res3;               //  4 Reserved for multi-user processing
    public byte m_mdxflag;             //  1 Production MDX flag, 0x01 if a production
                                       //    .MDX file exists for this table,
                                       //    0x00 if no .MDX file exist.
    public byte m_languagedriver;      //  1 Language driver ID
    public System.UInt16 m_res4;                //  2 Reserved, filled with zeros
    public char[] m_languagename;    // 32 Language driver name
    public System.UInt32 m_res5;               //  4 Reserved
                                        // 68 bytes

}

class DBASEFIELD
{
    public char[] m_fieldname;   // 32 Field name in ASCII (zero-filled)
    public byte m_fieldtype;       //  1 Field type in ASCII (B,C,D,N,L,M,@,I,+,F,O,G)
    public byte m_fieldlength;     //  1 Field length in binary
    public byte m_decimalcount;    //  1 Field decimal count in binary
    public System.UInt16 m_res1;            //  2 Reserved
    public byte m_mdxflag;         //  1 Production .MDX field flag; 0x01 if field has an index tag in the
                                   //    production .MDX file; 0x00 if the field is not indexed
    public System.UInt16 m_res2;            //  2 Reserved
    public System.UInt32 m_nextincrement;  //  4 Next autoincrement value, if the Field type is Autoincrement, 0x00 otherwise
    public System.UInt32 m_res3;           //  4 Reserved
                                    // 48 bytes
}

class DBASEPROPERTY
{
    public System.UInt16 m_numstdproperties;    // Number of standard properties
    public System.UInt16 m_sspda;               // Start of Standard Property Descriptor Array
    public System.UInt16 m_numcstmproperties;   // Number of custom properties
    public System.UInt16 m_scpda;               // Start of Custom Property Descriptor Array
    public System.UInt16 m_numriproperties;     // Number of Referential Integrity (RI) properties
    public System.UInt16 m_srpda;               // Start of RI Property Descriptor Array
    public System.UInt16 m_startofdata;         // Start of data
                                                // This points past the descriptor arrays to data used
                                                // by the arrays - for example, Custom property names
                                                // are stored here.
    public System.UInt16 m_sizeofstructure;     // Actual size of structure, including data
                                         // Note: in the .DBF this will be padded with zeros
                                         //       to the nearest 0x200, and may have 0x1A
                                         //       at the end.  If the structure contains RI
                                         //       data, it will not be padded.
};

// Standard Property Descriptor

class DBASESPD
{
    public System.UInt16 m_generation;      // Generational number
                                            // More than one value may exist for a property.
                                            // The current value is the value with the highest
                                            //   generational number.
    public System.UInt16 m_offset;          // Table field offset
                                            // base one.  01 for the first field in the table,
                                            //  02 for the second field, etc.  This is 0 in the
                                            //  case of a constraint.
    public byte m_property;        // Which property is described in this record
                                   // 8-bit number
                                   //  01 Required
                                   //  02 Min
                                   //  03 Max
                                   //  04 Default
                                   //  06 Database constraint
    public byte m_fieldtype;       // Field Type
                                   //  00 No type - constraint
                                   //  01 Char
                                   //  02 Numeric
                                   //  03 Memo
                                   //  04 Logical
                                   //  05 Date
                                   //  06 Float
                                   //  08 OLE
                                   //  09 Binary
                                   //  11 Long
                                   //  12 Timestamp
                                   //  13 Double
                                   //  14 AutoIncrement
    public byte m_constraint;      // 0x00 if the element is a constraint, 0x02 otherwise
    public System.UInt32 m_res1;           // Reserved
    public System.UInt16 m_dataoffset;      // Offset from the start of this structure to the data
                                            //  for the property.  The Required property has no data
                                            //  associated with it, so it is always 0.
    public System.UInt16 m_width;           // Width of database field associated with the property,
                                     //  and hence size of the data (includes 0 terminator
                                     //  in the case of a constraint).
};

// Custom Property Descriptor

class DBASECPD
{
    public System.UInt16 m_generation;      // Generational number
                                            // More than one value may exist for a property.
                                            // The current value is the value with the highest
                                            //   generational number.
    public System.UInt16 m_offset;          // Table field offset
                                            // base one.  01 for the first field in the table,
                                            //  02 for the second field, etc.  This is 0 in the
                                            //  case of a constraint.
    public byte m_fieldtype;       // Field Type
                                   //  00 No type - constraint
                                   //  01 Char
                                   //  02 Numeric
                                   //  03 Memo
                                   //  04 Logical
                                   //  05 Date
                                   //  06 Float
                                   //  08 OLE
                                   //  09 Binary
                                   //  11 Long
                                   //  12 Timestamp
                                   //  13 Double
                                   //  14 AutoIncrement
    public byte m_res1;            // Reserved
    public System.UInt16 m_nameoffset;      // Offset from the start of this structure to the Custom
                                            //  property name
    public System.UInt16 m_namelength;      // Length of the Custom property name
    public System.UInt16 m_dataoffset;      // Offset from the start of this structure to the Custom
                                            //  data for the property.
    public System.UInt16 m_datalength;      // Length of the Custom property data
                                     //  (does not include null terminator)
};

// Referential Integrity Property Descriptor Array
//   foreign = in the other table, local = in this table

class DBASERPD
{
    public byte m_pc;              // 0x07 if Master (parent), 0x08 if Dependent (child)
    public System.UInt16 m_number;          // Sequential number, 1 based counting, 0=dropped
    public System.UInt16 m_rinameoffset;    // Offset of the RI rule name - 0 terminated
    public System.UInt16 m_rinamesize;      // Size of the RI rule name
    public System.UInt16 m_ftnameoffset;    // Offset of the name of the Foreign table - 0 terminated
    public System.UInt16 m_ftnamesize;      // Size of the name of the Foreign table
    public byte m_behavior;        // Update and delete behavior
                                   //  0x10 Update Cascade
                                   //  0x01 Delete Cascade
    public System.UInt16 m_numfields;       // Number of fields in the linking key
    public System.UInt16 m_lttagoffset;     // Offset of the local table tag name - 0 terminated
    public System.UInt16 m_lttagsize;       // Size of the local table tag name
    public System.UInt16 m_fttagoffset;     // Offset of the foreign table tag name - 0 terminated
    public System.UInt16 m_fttagsize;		// Size of the foreign table tag name
};

class DbfReader
{
    BinaryReader Reader;

    public DbfReader(BinaryReader r)
    {
        Reader = r;
    }
    
    // For some reason the standard byte to int doesn't work with dbase formats
    public int ConvertByteToInt(byte[] bpSource)
    {
        int iResult = 0;
        bool bPositive = false;
        if ((bpSource[0] & 128) == 128)
        {
            bpSource[0] = (byte)(bpSource[0] ^ 0x80);
            bPositive = true;
        }

        Array.Reverse(bpSource);
        iResult = BitConverter.ToInt32(bpSource, 0);
        if (!bPositive) iResult *= -1;
        return (iResult);
    }

    public double ConvertByteToDouble(byte[] bpSource)
    {
        double iResult = 0;
        bool bPositive = false;
        if ((bpSource[0] & 128) == 128)
        {
            bpSource[0] = (byte)(bpSource[0] ^ 0x80);
            bPositive = true;
        }

        Array.Reverse(bpSource);
        iResult = BitConverter.ToDouble(bpSource, 0);
        if (!bPositive) iResult *= -1;
        return (iResult);

    }

    public DateTime ConvertByteToDateTime(byte[] bpSource)
    {
        // TODO: .Net DateTime does not support 4713 BC so... bleh... could do time?
        byte[] bpDate = new byte[4], bpTime = new byte[4];
        Array.Copy(bpSource, bpDate, 4);
        Array.ConstrainedCopy(bpSource,4, bpTime, 0,4);
        int d = ConvertByteToInt(bpDate);
        int t = ConvertByteToInt(bpTime);
        DateTime r = new DateTime();
        return (r);
    }


    public DBASEHEADER ReadHeader()
    {
        DBASEHEADER hdr = new DBASEHEADER();
        hdr.m_version = Reader.ReadByte();
        hdr.m_lcdyear = Reader.ReadByte();
        hdr.m_lcdmonth = Reader.ReadByte();
        hdr.m_lcdday = Reader.ReadByte();
        hdr.m_numrecords = Reader.ReadUInt32();
        hdr.m_headerlength = Reader.ReadUInt16();
        hdr.m_recordlength = Reader.ReadUInt16();
        hdr.m_resl = Reader.ReadUInt16();
        hdr.m_incompletetransaction = Reader.ReadByte();
        hdr.m_encryptionflag = Reader.ReadByte();
        hdr.m_freerecordthread = Reader.ReadUInt32();
        hdr.m_res2 = Reader.ReadUInt32();
        hdr.m_res3 = Reader.ReadUInt32();
        hdr.m_mdxflag = Reader.ReadByte();
        hdr.m_languagedriver = Reader.ReadByte();
        hdr.m_res4 = Reader.ReadUInt16();
        hdr.m_languagename = Reader.ReadChars(32);
        hdr.m_res5 = Reader.ReadUInt32();
        return (hdr);
    }

    public DBASEFIELD ReadField()
    {
        DBASEFIELD fld = new DBASEFIELD();
        fld.m_fieldname = Reader.ReadChars(32);
        fld.m_fieldtype = Reader.ReadByte();
        fld.m_fieldlength = Reader.ReadByte();
        fld.m_decimalcount = Reader.ReadByte();
        fld.m_res1 = Reader.ReadUInt16();
        fld.m_mdxflag = Reader.ReadByte();
        fld.m_res2 = Reader.ReadUInt16();
        fld.m_nextincrement = Reader.ReadUInt32();
        fld.m_res3 = Reader.ReadUInt32();
        return (fld);
    }

    public DBASEPROPERTY ReadProperty()
    {
        DBASEPROPERTY p = new DBASEPROPERTY();
        p.m_numstdproperties = Reader.ReadUInt16();
        p.m_sspda = Reader.ReadUInt16();
        p.m_numcstmproperties = Reader.ReadUInt16();
        p.m_scpda = Reader.ReadUInt16();
        p.m_numriproperties = Reader.ReadUInt16();
        p.m_srpda = Reader.ReadUInt16();
        p.m_startofdata = Reader.ReadUInt16();
        p.m_sizeofstructure = Reader.ReadUInt16();
        return (p);
    }

    // Standard Property Descriptor
    public DBASESPD ReadSPD()
    {
        DBASESPD s = new DBASESPD();
        s.m_generation = Reader.ReadUInt16();
        s.m_offset = Reader.ReadUInt16();
        s.m_property = Reader.ReadByte();
        s.m_fieldtype = Reader.ReadByte();
        s.m_constraint = Reader.ReadByte();
        s.m_res1 = Reader.ReadUInt32();
        s.m_dataoffset = Reader.ReadUInt16();
        s.m_width = Reader.ReadUInt16();
        return (s);
    }

    // Custom Property Descriptor
    public DBASECPD ReadCPD()
    {
        DBASECPD s = new DBASECPD();
        s.m_generation = Reader.ReadUInt16();
        s.m_offset = Reader.ReadUInt16();
        s.m_fieldtype = Reader.ReadByte();
        s.m_res1 = Reader.ReadByte();
        s.m_nameoffset = Reader.ReadUInt16();
        s.m_namelength = Reader.ReadUInt16();
        s.m_dataoffset = Reader.ReadUInt16();
        s.m_datalength = Reader.ReadUInt16();
        return (s);
    }

    // Referential Integrity Property Descriptor Array
    public DBASERPD ReadRPD()
    {
        DBASERPD s = new DBASERPD();
        s.m_pc = Reader.ReadByte();
        s.m_number = Reader.ReadUInt16();
        s.m_rinameoffset = Reader.ReadUInt16();
        s.m_rinamesize = Reader.ReadUInt16();
        s.m_ftnameoffset = Reader.ReadUInt16();
        s.m_ftnamesize = Reader.ReadUInt16();
        s.m_behavior = Reader.ReadByte();
        s.m_numfields = Reader.ReadUInt16();
        s.m_lttagoffset = Reader.ReadUInt16();
        s.m_lttagsize = Reader.ReadUInt16();
        s.m_fttagoffset = Reader.ReadUInt16();
        s.m_fttagsize = Reader.ReadUInt16();
        return (s);
    }

}


/* These are the old struct's
struct DBASEHEADER
{
    byte m_version;             //  1 dBASE version number
                                // Bits 0-2 indicate version number, 3=dBASE Level 5, 4=dBASE Level 7
                                // Bit 3 and bit 7 indicate presence of a dBASE IV or dBASE for Windows memo file
                                // Bits 4-6 indicate the presence of a dBASE IV SQL table
                                // Bit 7 indicates the presence of any .DBT memo file (either a dBASE III PLUS
                                //  type or a dBASE IV or dBASE for Windows memo file).
    byte m_lcdyear;             //  1 Year of last update
                                //    Add YY to 1900 (possible values 1900-2155)
    byte m_lcdmonth;            //  1 Month of last update
    byte m_lcdday;              //  1 Day of last update
    System.UInt32 m_numrecords;         //  4 Number of records in the table
                                //    least significant byte first
    System.UInt16 m_headerlength;        //  2 Number of bytes in the header
                                //    least significant byte first
    System.UInt16 m_recordlength;        //  2 Number of bytes in the record
                                //    least significant byte first
    System.UInt16 m_resl;                //  2 Reserved, filled with zeros
    byte m_incompletetransaction;   //  1 Flag indicating incomplete dBASE IV transaction
    byte m_encryptionflag;      //  1 dBASE IV encryption flag
    System.UInt32 m_freerecordthread;   //  4 Reserved for multi-user processing
    System.UInt32 m_res2;               //  4 Reserved for multi-user processing
    System.UInt32 m_res3;               //  4 Reserved for multi-user processing
    byte m_mdxflag;             //  1 Production MDX flag, 0x01 if a production
                                //    .MDX file exists for this table,
                                //    0x00 if no .MDX file exist.
    byte m_languagedriver;      //  1 Language driver ID
    System.UInt16 m_res4;                //  2 Reserved, filled with zeros
    string m_languagename[32];    // 32 Language driver name
    System.UInt32 m_res5;               //  4 Reserved
                                // 68 bytes
};

// Field descriptor array ??? 52

struct DBASEFIELD
{
    char m_fieldname[32];   // 32 Field name in ASCII (zero-filled)
    byte m_fieldtype;       //  1 Field type in ASCII (B,C,D,N,L,M,@,I,+,F,O,G)
    byte m_fieldlength;     //  1 Field length in binary
    byte m_decimalcount;    //  1 Field decimal count in binary
    System.UInt16 m_res1;            //  2 Reserved
    byte m_mdxflag;         //  1 Production .MDX field flag; 0x01 if field has an index tag in the
                            //    production .MDX file; 0x00 if the field is not indexed
    System.UInt16 m_res2;            //  2 Reserved
    System.UInt32 m_nextincrement;  //  4 Next autoincrement value, if the Field type is Autoincrement, 0x00 otherwise
    System.UInt32 m_res3;           //  4 Reserved
                            // 48 bytes
};

// Field properties structure

struct DBASEPROPERTY
{
    System.UInt16 m_numstdproperties;    // Number of standard properties
    System.UInt16 m_sspda;               // Start of Standard Property Descriptor Array
    System.UInt16 m_numcstmproperties;   // Number of custom properties
    System.UInt16 m_scpda;               // Start of Custom Property Descriptor Array
    System.UInt16 m_numriproperties;     // Number of Referential Integrity (RI) properties
    System.UInt16 m_srpda;               // Start of RI Property Descriptor Array
    System.UInt16 m_startofdata;         // Start of data
                                // This points past the descriptor arrays to data used
                                // by the arrays - for example, Custom property names
                                // are stored here.
    System.UInt16 m_sizeofstructure;     // Actual size of structure, including data
                                // Note: in the .DBF this will be padded with zeros
                                //       to the nearest 0x200, and may have 0x1A
                                //       at the end.  If the structure contains RI
                                //       data, it will not be padded.
};

// Standard Property Descriptor

struct DBASESPD
{
    System.UInt16 m_generation;      // Generational number
                            // More than one value may exist for a property.
                            // The current value is the value with the highest
                            //   generational number.
    System.UInt16 m_offset;          // Table field offset
                            // base one.  01 for the first field in the table,
                            //  02 for the second field, etc.  This is 0 in the
                            //  case of a constraint.
    byte m_property;        // Which property is described in this record
                            // 8-bit number
                            //  01 Required
                            //  02 Min
                            //  03 Max
                            //  04 Default
                            //  06 Database constraint
    byte m_fieldtype;       // Field Type
                            //  00 No type - constraint
                            //  01 Char
                            //  02 Numeric
                            //  03 Memo
                            //  04 Logical
                            //  05 Date
                            //  06 Float
                            //  08 OLE
                            //  09 Binary
                            //  11 Long
                            //  12 Timestamp
                            //  13 Double
                            //  14 AutoIncrement
    byte m_constraint;      // 0x00 if the element is a constraint, 0x02 otherwise
    System.UInt32 m_res1;           // Reserved
    System.UInt16 m_dataoffset;      // Offset from the start of this structure to the data
                            //  for the property.  The Required property has no data
                            //  associated with it, so it is always 0.
    System.UInt16 m_width;           // Width of database field associated with the property,
                            //  and hence size of the data (includes 0 terminator
                            //  in the case of a constraint).
};

// Custom Property Descriptor

struct DBASECPD
{
    System.UInt16 m_generation;      // Generational number
                            // More than one value may exist for a property.
                            // The current value is the value with the highest
                            //   generational number.
    System.UInt16 m_offset;          // Table field offset
                            // base one.  01 for the first field in the table,
                            //  02 for the second field, etc.  This is 0 in the
                            //  case of a constraint.
    byte m_fieldtype;       // Field Type
                            //  00 No type - constraint
                            //  01 Char
                            //  02 Numeric
                            //  03 Memo
                            //  04 Logical
                            //  05 Date
                            //  06 Float
                            //  08 OLE
                            //  09 Binary
                            //  11 Long
                            //  12 Timestamp
                            //  13 Double
                            //  14 AutoIncrement
    byte m_res1;            // Reserved
    System.UInt16 m_nameoffset;      // Offset from the start of this structure to the Custom
                            //  property name
    System.UInt16 m_namelength;      // Length of the Custom property name
    System.UInt16 m_dataoffset;      // Offset from the start of this structure to the Custom
                            //  data for the property.
    System.UInt16 m_datalength;      // Length of the Custom property data
                            //  (does not include null terminator)
};

// Referential Integrity Property Descriptor Array
//   foreign = in the other table, local = in this table

struct DBASERPD
{
    byte m_pc;              // 0x07 if Master (parent), 0x08 if Dependent (child)
    System.UInt16 m_number;          // Sequential number, 1 based counting, 0=dropped
    System.UInt16 m_rinameoffset;    // Offset of the RI rule name - 0 terminated
    System.UInt16 m_rinamesize;      // Size of the RI rule name
    System.UInt16 m_ftnameoffset;    // Offset of the name of the Foreign table - 0 terminated
    System.UInt16 m_ftnamesize;      // Size of the name of the Foreign table
    byte m_behavior;        // Update and delete behavior
                            //  0x10 Update Cascade
                            //  0x01 Delete Cascade
    System.UInt16 m_numfields;       // Number of fields in the linking key
    System.UInt16 m_lttagoffset;     // Offset of the local table tag name - 0 terminated
    System.UInt16 m_lttagsize;       // Size of the local table tag name
    System.UInt16 m_fttagoffset;     // Offset of the foreign table tag name - 0 terminated
    System.UInt16 m_fttagsize;		// Size of the foreign table tag name
};

*/

/*
	Property Data

	For standard properties, everything is stored exactly as it is in the Table records.
	Custom property data is stored as the Name string, followed immediately by the
	Value string, and a null terminator.  The Constraint text is stored as a null-
	terminated string.

	Table Records

	The records follow the header in the table file.  Data records are preceded by one
	byte, that is, a space(0x20) if the record is not deleted, an asterisk (0x2A) if
	the record is deleted.  Fields are packed into records without field seperators
	or record terminators.  The end of the file is marked by a single byte, with the
	end-of-file marker, an OEM code page character value of 25 (0x1A).

  */

