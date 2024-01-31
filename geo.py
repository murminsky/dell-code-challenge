import cartopy.crs as ccrs
import cartopy.feature as cfeature
import cartopy.io.shapereader as shpreader
import matplotlib.pyplot as plt
import matplotlib.pyplot as plt

ax = plt.axes(projection=ccrs.PlateCarree())
ax.add_feature(cfeature.LAND)
ax.add_feature(cfeature.OCEAN)
ax.add_feature(cfeature.COASTLINE)
ax.add_feature(cfeature.BORDERS, linestyle=':')


shpfilename = shpreader.natural_earth(resolution='110m', category='cultural', name= 'admin_0_countries')
reader = shpreader.Reader(shpfilename)
countries = reader.records()

def render_countries(country_list, title):
    for country in countries:
        print ('country is {}'.format(country))
        if country.attributes['NAME'] in country_list:
            ax.add_geometries(country.geometry, ccrs.PlateCarree(),facecolor=(0,0,1))
        else:
            pass
    plt.title(title)
    plt.show()