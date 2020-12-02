Please run the following command to populate the fits_data folder

```
wget https://www.spacetelescope.org/static/projects/fits_liberator/datasets/eagle/502nmos.zip
unzip -d hubble_samples 502nmos.zip 
rm 502nmos.zip
wget -P fits_samples https://fits.gsfc.nasa.gov/samples/WFPC2u5780205r_c0fx.fits
wget -P fits_samples https://fits.gsfc.nasa.gov/samples/FOCx38i0101t_c0f.fits
wget -P fits_samples https://fits.gsfc.nasa.gov/samples/FOSy19g0309t_c2f.fits
wget -P fits_samples https://fits.gsfc.nasa.gov/samples/HRSz0yd020fm_c2f.fits
wget -P fits_samples https://fits.gsfc.nasa.gov/samples/WFPC2ASSNu5780205bx.fits
wget -P tutorials http://data.astropy.org/tutorials/FITS-images/HorseHead.fits
```
