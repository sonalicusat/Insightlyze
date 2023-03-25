from django.contrib import admin
from django.urls import path
from . import views  # Change this line to import the correct view function
from django.conf.urls.static import static
from django.conf import settings


urlpatterns = [
    path('admin/', admin.site.urls),
    path('', views.index, name='index'),
    path('analyze', views.analyze, name='analyze'),
    #path('prior', lambda request: views.word_cloud_csv(request, 'D:/project/Insightlyze/Insightlyze/static/wc_prior.csv'), name='word_cloud_csv_prior'),
    path('event', lambda request: views.word_cloud_csv(request, 'D:/project/Insightlyze/Insightlyze/static/wc_event.csv'), name='word_cloud_csv_event'),
    #path('post', lambda request: views.word_cloud_csv(request, 'D:/project/Insightlyze/Insightlyze/static/wc_post.csv'), name='word_cloud_csv_post'),
    #path('histogram', lambda request: views.histogram_csv(request, 'D:/project/Insightlyze/Insightlyze/static/histogram.csv'), name='histogram_csv'),
] + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)