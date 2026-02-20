"""Pydantic models for listings and feed rows."""

from datetime import datetime

from pydantic import BaseModel, Field


class Listing(BaseModel):
    """Raw scraped listing data."""

    url: str
    lofty_id: str = ""
    mls_id: str = ""
    address: str = ""
    city: str = ""
    state: str = "CA"
    price: float = 0.0
    bedrooms: int = 0
    bathrooms: int = 0
    sqft: int = 0
    property_type: str = ""
    status: str = ""
    image_url: str = ""
    description: str = ""
    subdivision: str = ""
    scraped_at: datetime = Field(default_factory=datetime.utcnow)

    @property
    def is_active(self) -> bool:
        s = self.status.lower()
        # Lofty uses statuses like "Active", "New", "Open Sun 1PM-3PM", etc.
        inactive = ("sold", "closed", "pending", "withdrawn", "expired", "cancelled")
        return bool(s) and not any(x in s for x in inactive)

    @property
    def listing_name(self) -> str:
        """Human-readable name for feed, max 25 chars per Google Ads spec."""
        city_short = self.city.split(",")[0].strip() if self.city else ""
        # Try full format: "3BR Condo in Palm Desert"
        parts = []
        if self.bedrooms and self.bedrooms > 0:
            parts.append(f"{self.bedrooms}BR")
        if self.property_type:
            parts.append(self.property_type)
        if city_short:
            parts.append(f"in {city_short}")
        name = " ".join(parts) if parts else self.address
        # Truncate to 25 chars — drop city first, then property type
        if len(name) > 25 and city_short:
            parts_no_city = [p for p in parts if not p.startswith("in ")]
            name = " ".join(parts_no_city) if parts_no_city else self.address
        if len(name) > 25:
            name = name[:25]
        return name or self.address[:25]


class FeedRow(BaseModel):
    """Google Ads DynamicRealEstateAsset CSV row."""

    listing_id: str
    listing_name: str
    final_url: str
    image_url: str
    price: str  # "575000.00 USD"
    city_name: str = ""
    property_type: str = ""
    listing_type: str = "For Sale"
    address: str = ""
    description: str = ""
    contextual_keywords: str = ""

    @classmethod
    def from_listing(cls, listing: Listing) -> "FeedRow":
        keywords = []
        if listing.subdivision:
            keywords.append(listing.subdivision)
        if listing.sqft and listing.sqft > 0:
            keywords.append(f"{listing.sqft} sqft")

        desc = ""
        if listing.description:
            desc = listing.description.replace("\n", " ").replace("\r", " ").strip()[:25]

        return cls(
            listing_id=listing.lofty_id,
            listing_name=listing.listing_name,
            final_url=listing.url,
            image_url=listing.image_url,
            price=f"${int(listing.price):,}" if listing.price == int(listing.price) else f"${listing.price:,.2f}",
            city_name=listing.city.split(",")[0].strip()[:25] if listing.city else "",
            property_type=listing.property_type,
            address=f"{listing.address}, {listing.city}, {listing.state}" if listing.address and listing.city else listing.address,
            description=desc,
            contextual_keywords="; ".join(keywords),
        )


class StateEntry(BaseModel):
    """Per-listing state for incremental scraping."""

    url: str
    mls_id: str = ""
    last_scraped: datetime
    last_price: float = 0.0
    status: str = ""
