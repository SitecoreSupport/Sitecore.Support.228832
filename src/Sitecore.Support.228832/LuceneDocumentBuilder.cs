using Sitecore.ContentSearch;
using Sitecore.ContentSearch.Diagnostics;
using Sitecore.Data;
using Sitecore.Data.LanguageFallback;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace Sitecore.Support.ContentSearch.LuceneProvider
{
  public class LuceneDocumentBuilder : Sitecore.ContentSearch.LuceneProvider.LuceneDocumentBuilder
  {
    public LuceneDocumentBuilder(IIndexable indexable, IProviderUpdateContext context)
              : base(indexable, context)
    {
    }

    protected virtual bool IsValidFieldForIndexing(IIndexableDataField field)
    {
      return (field != null) && !string.IsNullOrWhiteSpace(field.Name) && !string.IsNullOrWhiteSpace(field.TypeKey);
    }

    public override void AddItemFields()
    {
      try
      {
        VerboseLogging.CrawlingLogDebug(() => "AddItemFields start");

        if (this.Options.IndexAllFields)
        {
          this.Indexable.LoadAllFields();
        }

        var loadedFields = new HashSet<string>(this.Indexable.Fields.Select(f => f.Id.ToString()));
        var includedFields = new HashSet<string>();
        if (this.Options.HasIncludedFields)
        {
          includedFields = new HashSet<string>(this.Options.IncludedFields);
        }
        includedFields.ExceptWith(loadedFields);

        if (IsParallel)
        {
          var exceptions = new ConcurrentQueue<Exception>();

          this.ParallelForeachProxy.ForEach(this.Indexable.Fields, this.ParallelOptions, f =>
          {
            try
            {
              this.CheckAndAddField(this.Indexable, f);
            }
            catch (Exception ex)
            {
              exceptions.Enqueue(ex);
            }
          });

          if (exceptions.Count > 0)
          {
            throw new AggregateException(exceptions);
          }

          if (!this.Options.IndexAllFields && this.Options.HasIncludedFields)
          {
            this.ParallelForeachProxy.ForEach(includedFields, this.ParallelOptions, fieldId =>
            {
              try
              {
                ID id;
                if (ID.TryParse(fieldId, out id))
                {
                  var field = this.Indexable.GetFieldById(id);
                  if (IsValidFieldForIndexing(field))
                  {
                    this.CheckAndAddField(this.Indexable, field);
                  }
                }
              }
              catch (Exception ex)
              {
                exceptions.Enqueue(ex);
              }
            });
          }
        }
        else
        {
          foreach (var field in this.Indexable.Fields)
          {
            this.CheckAndAddField(this.Indexable, field);
          }

          if (!this.Options.IndexAllFields && this.Options.HasIncludedFields)
          {
            foreach (var fieldId in includedFields)
            {
              ID id;
              if (ID.TryParse(fieldId, out id))
              {
                var field = this.Indexable.GetFieldById(id);
                if (IsValidFieldForIndexing(field))
                {
                  this.CheckAndAddField(this.Indexable, field);
                }
              }
            }
          }
        }
      }
      finally
      {
        VerboseLogging.CrawlingLogDebug(() => "AddItemFields End");
      }
    }

    private void CheckAndAddField(IIndexable indexable, IIndexableDataField field)
    {
      var fieldKey = field.Name;

      if (this.IsTemplate && this.Options.HasExcludedTemplateFields)
      {
        if (this.Options.ExcludedTemplateFields.Contains(fieldKey) || this.Options.ExcludedTemplateFields.Contains(field.Id.ToString()))
        {
          VerboseLogging.CrawlingLogDebug(() => string.Format("Skipping field id:{0}, name:{1}, typeKey:{2} - Field was excluded.", field.Id, field.Name, field.TypeKey));
          return;
        }
      }

      if (this.IsMedia && this.Options.HasExcludedMediaFields)
      {
        if (this.Options.ExcludedMediaFields.Contains(field.Name))
        {
          VerboseLogging.CrawlingLogDebug(() => string.Format("Skipping field id:{0}, name:{1}, typeKey:{2} - Media field was excluded.", field.Id, field.Name, field.TypeKey));
          return;
        }
      }

      if (this.Options.ExcludedFields.Contains(field.Id.ToString()) || this.Options.ExcludedFields.Contains(fieldKey))
      {
        VerboseLogging.CrawlingLogDebug(() => string.Format("Skipping field id:{0}, name:{1}, typeKey:{2} - Field was excluded.", field.Id, field.Name, field.TypeKey));
        return;
      }

      try
      {
        if (this.Options.IndexAllFields)
        {
          using (new LanguageFallbackFieldSwitcher(this.Index.EnableFieldLanguageFallback))
          {
            this.AddField(field);
          }
        }
        else
        {
          if (this.Options.IncludedFields.Contains(fieldKey) || this.Options.IncludedFields.Contains(field.Id.ToString()))
          {
            using (new LanguageFallbackFieldSwitcher(this.Index.EnableFieldLanguageFallback))
            {
              this.AddField(field);
            }
          }
          else
          {
            VerboseLogging.CrawlingLogDebug(() => string.Format("Skipping field id:{0}, name:{1}, typeKey:{2} - Field was not included.", field.Id, field.Name, field.TypeKey));
          }
        }
      }
      catch (Exception ex)
      {
        if (!this.Settings.StopOnCrawlFieldError())
          CrawlingLog.Log.Fatal(string.Format("Could not add field {1} : {2} for indexable {0}", indexable.UniqueId, field.Id, field.Name), ex);
        else
          throw;
      }
    }
  }
}