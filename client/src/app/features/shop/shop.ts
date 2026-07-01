import { Component, inject, OnInit, signal } from '@angular/core';
import { Product } from '../../shared/models/product';
import { Shop as ShopService } from '../../core/services/shop';
import { ProductItem } from './product-item/product-item';
import { MatDialog } from '@angular/material/dialog';
import { MatButton } from '@angular/material/button';
import { MatIcon } from '@angular/material/icon';
import { FiltersDialog } from './filters-dialog/filters-dialog';
import { MatListOption, MatSelectionList, MatSelectionListChange } from '@angular/material/list';
import { MatMenu, MatMenuTrigger } from '@angular/material/menu';
import { ShopParams } from '../../shared/models/shopParams';
import { MatPaginator, PageEvent } from '@angular/material/paginator';
import { Pagination } from '../../shared/models/pagination';
import { FormsModule } from '@angular/forms';
import { EmptyState } from '../../shared/components/empty-state/empty-state';

@Component({
  selector: 'app-shop',
  imports: [
    ProductItem,
    MatButton,
    MatIcon,
    MatMenu,
    MatMenuTrigger,
    MatSelectionList,
    MatListOption,
    MatPaginator,
    FormsModule,
    EmptyState
],
  templateUrl: './shop.html',
  styleUrl: './shop.css',
})
export class Shop implements OnInit {
  private shopService = inject(ShopService);
  private dialogService = inject(MatDialog);
  protected readonly products = signal<Pagination<Product> | null>(null);
  sortOptions = [
    { value: 'nameAsc', label: 'Name (A-Z)' },
    { value: 'priceAsc', label: 'Price (Low to High)' },
    { value: 'priceDesc', label: 'Price (High to Low)' }
  ];
  pageSizeOptions = [5, 10, 15, 20];
  shopParams = new ShopParams();

  ngOnInit(): void {
    this.initialiseShop();
  }

  initialiseShop() {
    this.shopService.getGenres();
    this.getProducts();
  }

  getProducts() {
    this.shopService.getProducts(this.shopParams).subscribe({
      next: (response) => this.products.set(response),
      error: (error) => console.error(error)
    });
  }

  handlePageEvent(event: PageEvent) {
    this.shopParams.pageIndex = event.pageIndex + 1;
    this.shopParams.pageSize = event.pageSize;
    this.getProducts();
  }

  onSearchChange() {
    this.shopParams.pageIndex = 1;
    this.getProducts();
  }

  onSortChange(event: MatSelectionListChange) {
    const selectedOption = event.options[0];
    if (selectedOption) {
      this.shopParams.sort = selectedOption.value;
      this.shopParams.pageIndex = 1;
      this.getProducts();
    }
  }

  openFiltersDialog() {
    const dialogRef = this.dialogService.open(FiltersDialog, {
      minWidth: '500px',
      autoFocus: false,
      data: {selectedGenres: this.shopParams.genres}
    });
    dialogRef.afterClosed().subscribe({
      next: (result) => {
        if (result) {
          this.shopParams.genres = result.selectedGenres;
          this.shopParams.pageIndex = 1;
          this.getProducts();
        }
      }
    });
  }

  resetFilters() {
    this.shopParams = new ShopParams();
    this.getProducts();
  }
}
