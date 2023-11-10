// See https://aka.ms/new-console-template for more information
using SharpCifs;
using NetCoreNtlmTest;
using System;

Console.WriteLine("Hello, World!");


//Config.SetProperty("jcifs.smb.client.domain", "DOMAIN");


var dbTest = new DbTest();
dbTest.test();
