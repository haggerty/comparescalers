void fourplots( Char_t *rootfile, Char_t *pngfile ) {

  //  gROOT->SetStyle("Plain");
  gStyle->SetOptStat(0);
  //  gStyle->SetTitleBorderSize(0);
  
  canvas = new TCanvas("canvas", "rc vs rhic",0,0,640,480);
  canvas->Divide(2,2,0.02,0.02,0);

  TFile *f;
  f = new TFile( rootfile );
  
  Int_t nplots = 4;

  TGraph *gr;
  for ( Int_t i = 1; i <= nplots ; i++ ) {
    canvas->cd(i);
    TString obj("g;");
    obj += i;
    cout << "key: " << obj << endl;
    f->GetObject(obj,gr);
    if ( gr != NULL ) {
      gr->Draw("AP");
      gr->GetYaxis()->SetTitleOffset(1.3);
    } else {
      cout << "key: " << obj << " has no graph"<< endl;
    }
  }

  canvas->Print( pngfile );
  
}
