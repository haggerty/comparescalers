#include "TSQLServer.h"
#include "TSQLResult.h"
#include "TSQLRow.h"
#include "Rtypes.h"
#include "TGraph.h"
#include "TStyle.h"
#include "TCanvas.h"
#include "TAxis.h"
#include "TFile.h"
#include "TMath.h"

#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <vector>

// This function is meant to compare the scalers in the trigger table of the pg daq database
// with the "RHIC" scalers in the rhicscaler1 table of the mysql scalers database

// John Haggerty, BNL, 2010.06.12

using namespace std;

  struct scaler {
    // get this stuff from the run and trigger (pg) databases
    Int_t runnumber;
    Char_t brtimestamp[20];
    Char_t ertimestamp[20];
    Double_t scalerberraw;
    // get this from the scaler (mysql) database
    Double_t rhicscalersum;
  } s;

string scaler_name( const Int_t which_scaler, const Int_t scaler_channel ) {
  
  // This function just gets the name of the scaler_value from the 
  // "table of contents table" toc
  
  TSQLServer *db = TSQLServer::Connect("mysql://phnxdb1.phenix.bnl.gov/scalers", "phoncs", "phenix7815");
  
  TString sql = "SELECT * FROM toc WHERE col = \"rate";
  sql += scaler_channel;
  if ( which_scaler == 1 ) { 
    sql += "\" AND tbl = \"scaler1\"";
  } else {
    sql += "\" AND tbl = \"scaler2\"";
  }    
  
  cout << "SQL query: " << sql << endl;

  TSQLResult *res;
  res = db->Query(sql);
  
  Int_t nrows = res->GetRowCount();
  //  cout << "Got " << nrows << " rows in result" << endl;
  if ( nrows != 1 ) return "";
  
  Int_t nfields = res->GetFieldCount();
  //  cout << "Got " << nfields << " fields in result" << endl;

  string s_name;

  TSQLRow *row;
  row = res->Next();
  for (Int_t j = 0; j < nfields; j++) {
    if ( strcmp(res->GetFieldName(j),"name") == 0 ) {
      s_name = (Char_t *) row->GetField(j);
    }
  }
  
  cout << "scaler_name: " << s_name << endl;

  delete row;
  delete res;
  delete db;

  return s_name;
}

Int_t comparescalers( Char_t *triggerconfig, Int_t start_runnumber, Int_t end_runnumber, Char_t *triggername, Int_t rhicscaler )
{
  
  std::vector<scaler> vs; 
  
  // get the scaler data from the run and trigger databases
  
  TSQLServer *daq_db = TSQLServer::Connect("pgsql://phnxdb1.phenix.bnl.gov/daq", "phnxrc", "");
  
  Char_t sql[8192];

  //  char* triggerconfig = "PP500Run11%"; // warning: deprecated conversion from string constant to 'char*'
  //  const char* triggerconfig = "PP500Run11%";

  //  const char* what = "run.runnumber,run.brtimestamp,run.ertimestamp,trigger.scalerberraw";
  const char* what = "run.runnumber,run.brtimestamp,run.ertimestamp,trigger.scalerberraw,trigger.scalerfraw";

  Char_t where[8192];
  sprintf( where, "lower(run.partitionname)='big' and lower(run.triggerconfig) LIKE lower('%s') and run.runtype='PHYSICS' and run.loggingon!='NO' and run.runstate='ENDED' and run.eventsinrun>'1000000' and run.runnumber>=%i and run.runnumber<=%i and (marked_invalid=0 or marked_invalid=1) and run.cermagnetcurrent>1650 and run.iermagnetcurrent>2400 and run.nermagnetcurrent>2875 and run.sermagnetcurrent>2250 and run.runnumber=trigger.runnumber and lower(trigger.name)=lower('%s')", triggerconfig, start_runnumber, end_runnumber, triggername );
  
  sprintf( sql, "SELECT DISTINCT %s FROM run,trigger WHERE %s ORDER BY runnumber",what,where); 

  cout << "SQL query: " << sql << endl;

  TSQLResult *res;
  res = daq_db->Query(sql);
  
  if ( res == NULL ) {
    cout << "Query failed! " << endl; 
    return -1;
  }

  Int_t nruns = res->GetRowCount();
  cout << nruns << " rows in result" << endl;
  Int_t nfields = res->GetFieldCount();
  //  cout << nfields << " fields in result" << endl;
  
  if ( nruns <= 0 ) {
    cout << "No runs in specified range" << endl;
    return -1;
  }

  TSQLRow *row;
  for (Int_t i = 0; i < nruns; i++) {

    row = res->Next();

    for (Int_t j = 0; j < nfields; j++) {
      // run number
      if ( strcmp(res->GetFieldName(j),"runnumber") == 0 ) {
	s.runnumber = strtol( row->GetField(j), NULL, 10 );
      }
      // begin run time
      if ( strcmp(res->GetFieldName(j),"brtimestamp") == 0 ) {
	strcpy( s.brtimestamp, row->GetField(j) );
      }
      // end run time
      if ( strcmp(res->GetFieldName(j),"ertimestamp") == 0 ) {
	strcpy( s.ertimestamp, row->GetField(j) );
      }
      // end run raw scaler
      //      if ( strcmp(res->GetFieldName(j),"scalerberraw") == 0 ) {
      if ( strcmp(res->GetFieldName(j),"scalerfraw") == 0 ) {
	unsigned long long int rs;
	rs = strtoull( row->GetField(j), NULL, 10 );
	s.scalerberraw = (Double_t) rs;
      }
    }
    
    // ugly special handling for a period when the ZDCLL1 was out of action in Run 11
    if ( s.runnumber >= 346178 && s.runnumber <= 346875 ) continue;

    s.rhicscalersum = 0.0;

    //    cout << "runnumber: " << s.runnumber;
    //    cout << " brtimestamp: " << s.brtimestamp << " ertimestamp: " << s.ertimestamp;
    //    cout << " scalerberraw: " << s.scalerberraw << endl;
    
    vs.push_back( s );
    
    delete row;
    
  }
  
  delete res;
  delete daq_db;
  
  // --------------------------------------------------------------------------------------------------
  
  // now iterate over the runs and get the corresponding scaler from the scaler database

  TSQLServer *scalers_db = TSQLServer::Connect("mysql://phnxdb1.phenix.bnl.gov/scalers", "phoncs", "phenix7815");
  
  // iterate over the runs we found above
  
  vector<scaler>::iterator its;
  
  for ( its=vs.begin(); its < vs.end(); its++ ) {
    s = *its;

    //    cout << "runnumber: " << s.runnumber;
    //    cout << " brtimestamp: " << s.brtimestamp << " ertimestamp: " << s.ertimestamp;
    //    cout << " scalerberraw: " << s.scalerberraw << endl;
    
    // the IFNULL protects the returned data against NULL returned data
    sprintf(sql,"SELECT IFNULL(sum(rate%i),0.0) AS rhicscalersum FROM rhicscaler1 WHERE read_datetime >= \"%s\" AND read_datetime <= \"%s\" ORDER BY id ASC",rhicscaler,s.brtimestamp,s.ertimestamp);    
    
    //    cout << "SQL query: " << sql << endl;
    
    res = scalers_db->Query(sql);
    Int_t nrows = res->GetRowCount();
    //    cout << nrows << " rows in result" << endl;
    nfields = res->GetFieldCount();
    //    cout << nfields << " fields in result" << endl;
    
    for (Int_t i = 0; i < nrows; i++) {
      row = res->Next();
      for (Int_t j = 0; j < nfields; j++) {
	// rhicscalersum
	if ( strcmp(res->GetFieldName(j),"rhicscalersum") == 0 ) {
	  s.rhicscalersum = 60.0*strtod( row->GetField(j), NULL );
	}
      }
    }

    // print out some anomalies if you like
    if ( s.scalerberraw > 1.0e6 ) {
      Double_t discrepancy = TMath::Abs( (s.rhicscalersum/s.scalerberraw) - 1.0 );
      if ( discrepancy > 0.25 ) {
    	cout << "runnumber: " << s.runnumber; 
    	cout << " scalerberraw: " << s.scalerberraw;
    	cout << " rhicscalersum: " << s.rhicscalersum << endl;
      }
    }


    // this puts the new value (rhicscalersum) back into the vector
    *its = s;
    delete row;
  }

  delete res;
  delete scalers_db;
  
  cout << "drawing graph" << endl;

  // now the vector is filled with what I wanted from the database

  Double_t *xx;
  Double_t *yy;

  xx = new Double_t[nruns];
  yy = new Double_t[nruns];

  Int_t ipt = 0;
  for ( its=vs.begin(); its < vs.end(); its++ ) {
    s = *its;
    //    cout << "runnumber: " << s.runnumber;
    //    cout << " brtimestamp: " << s.brtimestamp << " ertimestamp: " << s.ertimestamp;
    //    cout << " scalerberraw: " << s.scalerberraw;
    //    cout << " rhicscalersum: " << s.rhicscalersum << endl;
    
    xx[ipt] = s.scalerberraw/1e6;
    yy[ipt] = s.rhicscalersum/1e6;
    
    ipt++;
  }

  // now the graph

  TCanvas *canvas;
  canvas = new TCanvas("canvas", "Scaler comparison",0,0,640,480);
  gStyle->SetOptStat(0);
  gStyle->SetTitleBorderSize(0);

  TGraph *gr;
  gr = new TGraph( nruns, xx, yy );

  TString title = "Run Control vs. RHIC scalers runs ";
  title += start_runnumber;
  title += "-";
  title += end_runnumber;
  gr->SetTitle( title );
  
  TString xtitle = "run control ";
  xtitle += triggername;
  xtitle += " (M)";
  gr->GetXaxis()->SetTitle( xtitle );

  TString ytitle = "RHIC scaler ";
  ytitle += rhicscaler;
  ytitle += " ";
  ytitle += scaler_name(1,rhicscaler);
  ytitle += " (M)";
  gr->GetYaxis()->SetTitle( ytitle );
  gr->GetYaxis()->SetTitleOffset( 1.3 );

  gr->Draw("AP");
  
  gr->SetMarkerStyle(20); 
  gr->SetMarkerSize(0.30); 
  gr->SetMarkerColor( 1 ); 

  gr->SetName( "g" );

  canvas->Update();
  canvas->Modified();

  //  Char_t rootfilename[80];
  //  sprintf( rootfilename, "comparescalers.root");
  TString rootfilename = triggerconfig;
  rootfilename += ".root";
  cout << "root file: " << rootfilename << endl;

  TFile *f;
  f = new TFile( rootfilename, "UPDATE");
  gr->Write();
  f->Close();

  delete [] xx;
  delete [] yy;

  return 0;

}

int main(int argc, char* argv[]) 
{
  
  if ( argc != 6 ) {
    cout << "usage: comparescalers start_run end_run triggerconfig triggername rhic_scaler" << endl;
    cout << "example: comparescalers 330693 340515 pp500run11 zdcll1wide 6" << endl;
    return -1;
  }

  Int_t start_runnumber = strtol( argv[1], NULL, 10 );
  Int_t end_runnumber = strtol( argv[2], NULL, 10 );
  Int_t rhicscaler = strtol( argv[5], NULL, 10 );;

  cout << "run numbers: " << start_runnumber << "-" << end_runnumber;
  cout << " triggername: " << argv[4] << " rhic scaler: " << rhicscaler << endl;

  comparescalers( argv[3], start_runnumber, end_runnumber, argv[4], rhicscaler );

}
