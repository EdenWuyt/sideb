import { Component, inject } from '@angular/core';
import { Shop as ShopService } from '../../../core/services/shop';
import { MatDivider, MatListOption, MatSelectionList } from '@angular/material/list';
import { MatButton } from '@angular/material/button';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { FormsModule } from '@angular/forms';

@Component({
  selector: 'app-filters-dialog',
  imports: [
    MatDivider, 
    MatSelectionList,
    MatListOption,
    MatButton,
    FormsModule
  ],
  templateUrl: './filters-dialog.html',
  styleUrl: './filters-dialog.css',
})
export class FiltersDialog {
  protected shopService = inject(ShopService);
  private dialogRef = inject(MatDialogRef<FiltersDialog>);
  data = inject(MAT_DIALOG_DATA);
  selectedGenres: string[] = this.data.selectedGenres;

  applyFilters() {
    this.dialogRef.close({
      selectedGenres: this.selectedGenres
    });
  }

  clearFilters() {
    this.selectedGenres = [];
  }
}
